"""Flask server for Medical Logistics NBA App."""
import os
import json
import time
import queue
import logging
from datetime import datetime
from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DIST_FOLDER = os.path.join(os.path.dirname(__file__), "dist")
if not os.path.exists(DIST_FOLDER):
    os.makedirs(DIST_FOLDER)
    with open(os.path.join(DIST_FOLDER, "index.html"), "w") as f:
        f.write("<html><body><h1>Building...</h1><p>React app is building. Refresh in a moment.</p></body></html>")

app = Flask(__name__, static_folder="dist", static_url_path="")
CORS(app)

CATALOG = os.environ.get("CATALOG", "eswanson_demo")
SCHEMA = os.environ.get("SCHEMA", "med_logistics_nba")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
ANALYSIS_TABLE = f"{CATALOG}.{SCHEMA}.analysis_outputs"
ENCOUNTERS_TABLE = f"{CATALOG}.{SCHEMA}.dim_encounters"
DRUG_COSTS_TABLE = f"{CATALOG}.{SCHEMA}.fact_drug_costs"
ED_WAIT_TABLE = f"{CATALOG}.{SCHEMA}.fact_ed_wait_times"
STAFFING_TABLE = f"{CATALOG}.{SCHEMA}.fact_staffing"
KPI_TABLE = f"{CATALOG}.{SCHEMA}.fact_operational_kpis"

_workspace_client = None
_agent_loaded = False
_invoke_agent = None
_invoke_deep_streaming = None
_get_autonomous = None
_start_autonomous = None


def get_workspace_client():
    global _workspace_client
    if _workspace_client is None:
        from databricks.sdk import WorkspaceClient
        _workspace_client = WorkspaceClient()
    return _workspace_client


def load_agent():
    global _agent_loaded, _invoke_agent, _invoke_deep_streaming, _get_autonomous, _start_autonomous
    if _agent_loaded:
        return True
    try:
        from agent.graph import invoke_agent, invoke_deep_agent_streaming
        from agent.autonomous import get_autonomous, start_autonomous
        _invoke_agent = invoke_agent
        _invoke_deep_streaming = invoke_deep_agent_streaming
        _get_autonomous = get_autonomous
        _start_autonomous = start_autonomous
        _agent_loaded = True
        logger.info("Agent module loaded successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to load agent module: {e}")
        return False


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve_react_app(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/health", methods=["GET"])
def health_check():
    status = {"status": "ok", "timestamp": datetime.utcnow().isoformat(), "agent_loaded": _agent_loaded}
    if _agent_loaded and _get_autonomous:
        try:
            status["autonomous"] = _get_autonomous().get_status()
        except Exception as e:
            status["autonomous_error"] = str(e)
    return jsonify(status)


@app.route("/api/config", methods=["GET"])
def get_config():
    return jsonify({"catalog": CATALOG, "schema": SCHEMA,
                    "warehouse_id": WAREHOUSE_ID[:8] + "..." if WAREHOUSE_ID else None})


@app.route("/api/agent/chat", methods=["POST"])
def agent_chat():
    if not load_agent():
        return jsonify({"error": "Agent not available"}), 503
    try:
        data = request.get_json()
        message = data.get("message", "")
        mode = data.get("mode", "orchestrator")
        history = data.get("history", [])
        stream = data.get("stream", False)
        if not message:
            return jsonify({"error": "No message provided"}), 400

        if stream and mode == "rag":
            return _stream_deep_analysis(message, history)

        result = _invoke_agent(message=message, mode=mode, history=history)
        return jsonify({"response": result.get("response", ""), "tool_calls": result.get("tool_calls", []),
                        "mode": result.get("mode", mode), "timestamp": datetime.utcnow().isoformat()})
    except Exception as e:
        logger.error(f"Agent chat error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


def _stream_deep_analysis(message, history):
    """SSE streaming response for deep analysis mode with keepalive heartbeat."""
    progress_queue = _invoke_deep_streaming(message=message, history=history)

    def generate():
        start = time.time()
        while time.time() - start < 300:
            try:
                event = progress_queue.get(timeout=5)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("stage") in ("done", "error"):
                    return
            except queue.Empty:
                yield ": keepalive\n\n"
        yield f"data: {json.dumps({'stage': 'error', 'message': 'Analysis timed out after 5 minutes.'})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# Autonomous mode endpoints
@app.route("/api/autonomous/status", methods=["GET"])
def autonomous_status():
    if not load_agent():
        return jsonify({"error": "Agent not available", "is_running": False}), 200
    return jsonify(_get_autonomous().get_status())


@app.route("/api/autonomous/start", methods=["POST"])
def autonomous_start():
    if not load_agent():
        return jsonify({"error": "Agent not available"}), 503
    try:
        _start_autonomous()
        return jsonify({"success": True, "status": _get_autonomous().get_status()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/autonomous/stop", methods=["POST"])
def autonomous_stop():
    if not load_agent():
        return jsonify({"error": "Agent not available"}), 503
    try:
        _get_autonomous().stop()
        return jsonify({"success": True, "status": _get_autonomous().get_status()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/autonomous/config", methods=["PATCH"])
def autonomous_config():
    if not load_agent():
        return jsonify({"error": "Agent not available"}), 503
    try:
        data = request.get_json()
        autonomous = _get_autonomous()
        if "interval_seconds" in data:
            autonomous.set_interval(data["interval_seconds"])
        if "capabilities" in data:
            config = {c["id"]: c["enabled"] for c in data["capabilities"]}
            autonomous.set_capabilities(config)
        return jsonify({"success": True, "status": autonomous.get_status()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/autonomous/trigger", methods=["POST"])
def autonomous_trigger():
    if not load_agent():
        return jsonify({"error": "Agent not available"}), 503
    try:
        data = request.get_json() or {}
        capability = data.get("capability")
        result = _get_autonomous().trigger_now(capability)
        return jsonify({"success": True, "result": result, "status": _get_autonomous().get_status()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/autonomous/check-now", methods=["POST"])
def autonomous_check_now():
    """Trigger an immediate health check (and NBA if issues found)."""
    if not load_agent():
        return jsonify({"error": "Agent not available"}), 503
    try:
        import threading
        threading.Thread(target=_get_autonomous()._autonomous_job, daemon=True).start()
        return jsonify({"success": True, "message": "Health check triggered"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/data/inject-anomaly", methods=["POST"])
def inject_anomaly():
    """Insert anomalous encounters for demo/testing -- high LOS, high cost readmissions."""
    try:
        w = get_workspace_client()
        import random
        from datetime import timedelta
        ts = int(datetime.utcnow().timestamp())
        rows = []
        hospitals = ["Hospital_A", "Hospital_B", "Hospital_C"]
        depts = ["Cardiology", "Orthopedics", "General_Medicine", "Neurology"]
        for i in range(10):
            eid = f"ANOM_{ts}_{i:03d}"
            hosp = random.choice(hospitals)
            dept = random.choice(depts)
            los = random.randint(10, 25)
            admit = (datetime.utcnow() - timedelta(days=los)).strftime("%Y-%m-%d")
            discharge = datetime.utcnow().strftime("%Y-%m-%d")
            rows.append(
                f"('{eid}', 'PAT_{ts}_{i}', '{hosp}', '{dept}', 'Medicare', "
                f"'{admit}', '{discharge}', {los}, true, 'Dr. Demo')"
            )
        insert_sql = f"""INSERT INTO {ENCOUNTERS_TABLE}
            (encounter_id, patient_id, hospital, department, payer,
             admit_date, discharge_date, los_days, is_readmission, attending_physician)
            VALUES {', '.join(rows)}"""
        w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=insert_sql, wait_timeout="30s")
        return jsonify({"success": True, "injected": len(rows)})
    except Exception as e:
        logger.error(f"Inject anomaly error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/data/inject-good", methods=["POST"])
def inject_good_data():
    """Insert healthy encounters -- short LOS, no readmissions."""
    try:
        w = get_workspace_client()
        import random
        from datetime import timedelta
        ts = int(datetime.utcnow().timestamp())
        rows = []
        hospitals = ["Hospital_A", "Hospital_B", "Hospital_C"]
        depts = ["Cardiology", "Orthopedics", "General_Medicine", "Neurology", "Pediatrics"]
        for i in range(10):
            eid = f"GOOD_{ts}_{i:03d}"
            hosp = random.choice(hospitals)
            dept = random.choice(depts)
            los = random.randint(1, 3)
            admit = (datetime.utcnow() - timedelta(days=los)).strftime("%Y-%m-%d")
            discharge = datetime.utcnow().strftime("%Y-%m-%d")
            rows.append(
                f"('{eid}', 'PAT_{ts}_{i}', '{hosp}', '{dept}', 'BlueCross', "
                f"'{admit}', '{discharge}', {los}, false, 'Dr. Good')"
            )
        insert_sql = f"""INSERT INTO {ENCOUNTERS_TABLE}
            (encounter_id, patient_id, hospital, department, payer,
             admit_date, discharge_date, los_days, is_readmission, attending_physician)
            VALUES {', '.join(rows)}"""
        w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=insert_sql, wait_timeout="30s")
        return jsonify({"success": True, "injected": len(rows)})
    except Exception as e:
        logger.error(f"Inject good data error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


# Legacy heartbeat endpoints
@app.route("/api/heartbeat/status", methods=["GET"])
def heartbeat_status():
    return autonomous_status()

@app.route("/api/heartbeat/start", methods=["POST"])
def heartbeat_start():
    return autonomous_start()

@app.route("/api/heartbeat/stop", methods=["POST"])
def heartbeat_stop():
    return autonomous_stop()


# Health score endpoint
@app.route("/api/health/score", methods=["GET"])
def get_health_score():
    try:
        w = get_workspace_client()
        # Average LOS (lower is better, target < 5)
        los_query = f"SELECT AVG(los_days) as avg_los FROM {ENCOUNTERS_TABLE} WHERE admit_date > CURRENT_TIMESTAMP - INTERVAL 30 DAYS"
        # ED wait breaches
        ed_query = f"SELECT COUNT(*) as breach_count FROM {ED_WAIT_TABLE} WHERE wait_minutes > 60 AND arrival_time > CURRENT_TIMESTAMP - INTERVAL 7 DAYS"
        # Readmission rate
        readmit_query = f"""
        SELECT ROUND(SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as readmit_rate
        FROM {ENCOUNTERS_TABLE}
        """
        los_result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=los_query, wait_timeout="30s")
        ed_result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=ed_query, wait_timeout="30s")
        readmit_result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=readmit_query, wait_timeout="30s")

        avg_los = 4.5
        if los_result.result and los_result.result.data_array:
            val = los_result.result.data_array[0][0]
            avg_los = float(val) if val else 4.5

        ed_breaches = 0
        if ed_result.result and ed_result.result.data_array:
            val = ed_result.result.data_array[0][0]
            ed_breaches = int(val) if val else 0

        readmit_rate = 8.0
        if readmit_result.result and readmit_result.result.data_array:
            val = readmit_result.result.data_array[0][0]
            readmit_rate = float(val) if val else 8.0

        # Health score: target LOS<5, readmit<10%, few ED breaches
        los_score = max(0, min(40, 40 - (avg_los - 4.0) * 10))
        readmit_score = max(0, min(30, 30 - (readmit_rate - 5.0) * 3))
        ed_score = max(0, min(30, 30 - ed_breaches * 0.5))
        health_score = int(los_score + readmit_score + ed_score)
        health_score = max(0, min(100, health_score))

        total_encounters = 0
        try:
            enc_result = w.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID, statement=f"SELECT COUNT(*) FROM {ENCOUNTERS_TABLE}", wait_timeout="30s")
            if enc_result.result and enc_result.result.data_array:
                total_encounters = int(enc_result.result.data_array[0][0] or 0)
        except:
            pass

        summary = f"{total_encounters} encounters tracked, avg LOS {avg_los:.1f} days. "
        if health_score >= 80:
            summary += "Operations running well."
        elif health_score >= 60:
            summary += "Some attention needed."
        else:
            summary += "Critical issues detected."

        return jsonify({"score": health_score, "avg_los": round(avg_los, 1), "readmission_rate": round(readmit_rate, 1),
                        "ed_breaches": ed_breaches, "total_encounters": total_encounters, "summary": summary})
    except Exception as e:
        logger.error(f"Health score error: {e}", exc_info=True)
        return jsonify({"score": None, "error": str(e)}), 200


@app.route("/api/alerts/active", methods=["GET"])
def get_alerts():
    try:
        w = get_workspace_client()
        # High LOS alerts
        query = f"""
        SELECT hospital, department, ROUND(AVG(los_days), 1) as avg_los, COUNT(*) as enc_count
        FROM {ENCOUNTERS_TABLE}
        WHERE admit_date > CURRENT_TIMESTAMP - INTERVAL 7 DAYS
        GROUP BY hospital, department
        HAVING AVG(los_days) > 6.0
        ORDER BY avg_los DESC LIMIT 5
        """
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        alerts = []
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            for row in result.result.data_array:
                data = dict(zip(columns, row))
                avg_los = float(data.get("avg_los", 0) or 0)
                alerts.append({
                    "id": f"los_{data['hospital']}_{data['department']}",
                    "severity": "high" if avg_los > 8 else "medium",
                    "title": f"High LOS: {data['hospital']} / {data['department']}",
                    "detail": f"Avg {avg_los} days across {data.get('enc_count', 0)} encounters this week"
                })
        return jsonify({"alerts": alerts})
    except Exception as e:
        logger.error(f"Alerts error: {e}", exc_info=True)
        return jsonify({"alerts": [], "error": str(e)}), 200


@app.route("/api/analysis/latest", methods=["GET"])
def get_latest_analysis():
    try:
        w = get_workspace_client()
        limit = request.args.get("limit", 10, type=int)
        analysis_type = request.args.get("type")
        where_clause = f"WHERE analysis_type = '{analysis_type}'" if analysis_type else ""
        query = f"""SELECT id, encounter_id, analysis_type, insights, recommendations, created_at, agent_mode
        FROM {ANALYSIS_TABLE} {where_clause} ORDER BY created_at DESC LIMIT {limit}"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        analyses = []
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            for row in result.result.data_array:
                analyses.append(dict(zip(columns, row)))
        return jsonify({"analyses": analyses, "count": len(analyses)})
    except Exception as e:
        return jsonify({"error": str(e), "analyses": []}), 200


@app.route("/api/recommendations/pending", methods=["GET"])
def get_pending_recommendations():
    try:
        w = get_workspace_client()
        limit = request.args.get("limit", 20, type=int)
        query = f"""SELECT id, encounter_id, analysis_type, insights, recommendations, created_at, priority, status
        FROM {ANALYSIS_TABLE} WHERE status = 'pending' AND recommendations IS NOT NULL
        ORDER BY CASE priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 WHEN 'low' THEN 4 ELSE 5 END,
        created_at DESC LIMIT {limit}"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        recommendations = []
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            for row in result.result.data_array:
                recommendations.append(dict(zip(columns, row)))
        return jsonify({"recommendations": recommendations})
    except Exception as e:
        return jsonify({"error": str(e), "recommendations": []}), 200


@app.route("/api/recommendations/<rec_id>/approve", methods=["POST"])
def approve_recommendation(rec_id):
    try:
        data = request.json or {}
        reviewed_by = data.get("reviewed_by", "unknown")
        engineer_notes = data.get("engineer_notes", "")
        w = get_workspace_client()
        query = f"""UPDATE {ANALYSIS_TABLE} SET status = 'approved', reviewed_by = '{reviewed_by}',
        reviewed_at = current_timestamp(), engineer_notes = '{engineer_notes.replace("'", "''")}' WHERE id = '{rec_id}'"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        if result.status.state.value in ["SUCCEEDED", "CLOSED"]:
            return jsonify({"success": True, "id": rec_id, "status": "approved"})
        return jsonify({"error": f"Update failed: {result.status.error}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/recommendations/<rec_id>/reject", methods=["POST"])
def reject_recommendation(rec_id):
    try:
        data = request.json or {}
        reviewed_by = data.get("reviewed_by", "unknown")
        engineer_notes = data.get("engineer_notes", "")
        w = get_workspace_client()
        query = f"""UPDATE {ANALYSIS_TABLE} SET status = 'rejected', reviewed_by = '{reviewed_by}',
        reviewed_at = current_timestamp(), engineer_notes = '{engineer_notes.replace("'", "''")}' WHERE id = '{rec_id}'"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        if result.status.state.value in ["SUCCEEDED", "CLOSED"]:
            return jsonify({"success": True, "id": rec_id, "status": "rejected"})
        return jsonify({"error": f"Update failed: {result.status.error}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/recommendations/<rec_id>/export-pdf", methods=["POST"])
def export_recommendation_pdf(rec_id):
    try:
        w = get_workspace_client()
        query = f"""SELECT id, encounter_id, analysis_type, insights, recommendations, created_at, priority, status
        FROM {ANALYSIS_TABLE} WHERE id = '{rec_id}'"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            rec = dict(zip(columns, result.result.data_array[0]))
            report = f"""
NEXT BEST ACTION REPORT
{'='*60}

Recommendation ID: {rec['id']}
Encounter ID: {rec.get('encounter_id', 'N/A')}
Analysis Type: {rec['analysis_type']}
Priority: {rec.get('priority', 'Not Set').upper()}
Created: {rec['created_at']}
Status: {rec['status']}

FINDINGS
{'-'*60}
{rec['insights']}

RECOMMENDATIONS
{'-'*60}
{rec.get('recommendations', 'No recommendations provided')}

SIGN-OFF
{'-'*60}
Administrator Signature: ______________________  Date: __________
Notes:


{'='*60}
End of Report
"""
            return jsonify({"success": True, "report": report, "filename": f"recommendation_{rec_id}.txt"})
        return jsonify({"error": "Recommendation not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ed/summary", methods=["GET"])
def get_ed_summary():
    try:
        w = get_workspace_client()
        query = f"""SELECT acuity_level,
            ROUND(AVG(wait_minutes), 1) as avg_wait,
            COUNT(*) as total,
            SUM(CASE WHEN (acuity_level <= 2 AND wait_minutes > 15) OR (acuity_level > 2 AND wait_minutes > 60) THEN 1 ELSE 0 END) as breaches
        FROM {ED_WAIT_TABLE}
        WHERE arrival_time >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
        GROUP BY acuity_level ORDER BY acuity_level"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        levels = []
        total_breaches = 0
        if result.result and result.result.data_array:
            cols = [c.name for c in result.manifest.schema.columns]
            for row in result.result.data_array:
                d = dict(zip(cols, row))
                b = int(d.get("breaches") or 0)
                total_breaches += b
                levels.append({"acuity": int(d.get("acuity_level", 0)),
                               "avg_wait": float(d.get("avg_wait") or 0),
                               "total": int(d.get("total") or 0), "breaches": b})
        return jsonify({"levels": levels, "total_breaches": total_breaches})
    except Exception as e:
        return jsonify({"error": str(e), "levels": [], "total_breaches": 0}), 200


@app.route("/api/drugs/summary", methods=["GET"])
def get_drugs_summary():
    try:
        w = get_workspace_client()
        total_q = f"SELECT ROUND(SUM(total_cost), 2) as total_spend FROM {DRUG_COSTS_TABLE} WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAYS"
        cat_q = f"""SELECT drug_category, ROUND(SUM(total_cost), 2) as spend
            FROM {DRUG_COSTS_TABLE} WHERE order_date >= CURRENT_DATE - INTERVAL 30 DAYS
            GROUP BY drug_category ORDER BY spend DESC LIMIT 3"""
        total_r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=total_q, wait_timeout="30s")
        cat_r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=cat_q, wait_timeout="30s")
        total_spend = 0
        if total_r.result and total_r.result.data_array:
            total_spend = float(total_r.result.data_array[0][0] or 0)
        categories = []
        if cat_r.result and cat_r.result.data_array:
            cols = [c.name for c in cat_r.manifest.schema.columns]
            for row in cat_r.result.data_array:
                d = dict(zip(cols, row))
                categories.append({"category": d.get("drug_category"), "spend": float(d.get("spend") or 0)})
        return jsonify({"total_spend": total_spend, "categories": categories})
    except Exception as e:
        return jsonify({"error": str(e), "total_spend": 0, "categories": []}), 200


@app.route("/api/staffing/summary", methods=["GET"])
def get_staffing_summary():
    try:
        w = get_workspace_client()
        query = f"""SELECT department,
            ROUND(SUM(CASE WHEN staff_type = 'contract' THEN fte_count ELSE 0 END) * 100.0 /
                  NULLIF(SUM(fte_count), 0), 1) as contract_pct,
            SUM(fte_count) as total_fte
        FROM {STAFFING_TABLE}
        WHERE report_date >= CURRENT_DATE - INTERVAL 30 DAYS
        GROUP BY department ORDER BY contract_pct DESC"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        departments = []
        overall_contract = 0
        overall_total = 0
        if result.result and result.result.data_array:
            cols = [c.name for c in result.manifest.schema.columns]
            for row in result.result.data_array:
                d = dict(zip(cols, row))
                pct = float(d.get("contract_pct") or 0)
                fte = int(d.get("total_fte") or 0)
                overall_contract += pct * fte / 100
                overall_total += fte
                departments.append({"department": d.get("department"), "contract_pct": pct, "total_fte": fte})
        overall_pct = round(overall_contract / overall_total * 100, 1) if overall_total else 0
        return jsonify({"overall_contract_pct": overall_pct, "departments": departments[:5]})
    except Exception as e:
        return jsonify({"error": str(e), "overall_contract_pct": 0, "departments": []}), 200


@app.route("/api/encounters/summary", methods=["GET"])
def get_encounter_summary():
    try:
        w = get_workspace_client()
        query = f"""SELECT COUNT(*) as total_encounters,
               SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) as readmissions,
               COUNT(DISTINCT hospital) as hospital_count,
               ROUND(AVG(los_days), 1) as avg_los
        FROM {ENCOUNTERS_TABLE}"""
        trend_query = f"""SELECT
            ROUND(AVG(CASE WHEN admit_date > CURRENT_TIMESTAMP - INTERVAL 7 DAYS THEN los_days END), 2) as this_week_los,
            ROUND(AVG(CASE WHEN admit_date BETWEEN CURRENT_TIMESTAMP - INTERVAL 14 DAYS AND CURRENT_TIMESTAMP - INTERVAL 7 DAYS THEN los_days END), 2) as last_week_los,
            SUM(CASE WHEN admit_date > CURRENT_TIMESTAMP - INTERVAL 7 DAYS AND is_readmission THEN 1 ELSE 0 END) as this_week_readmits,
            SUM(CASE WHEN admit_date BETWEEN CURRENT_TIMESTAMP - INTERVAL 14 DAYS AND CURRENT_TIMESTAMP - INTERVAL 7 DAYS AND is_readmission THEN 1 ELSE 0 END) as last_week_readmits,
            SUM(CASE WHEN admit_date > CURRENT_TIMESTAMP - INTERVAL 7 DAYS THEN 1 ELSE 0 END) as this_week_enc,
            SUM(CASE WHEN admit_date BETWEEN CURRENT_TIMESTAMP - INTERVAL 14 DAYS AND CURRENT_TIMESTAMP - INTERVAL 7 DAYS THEN 1 ELSE 0 END) as last_week_enc
        FROM {ENCOUNTERS_TABLE}
        WHERE admit_date > CURRENT_TIMESTAMP - INTERVAL 14 DAYS"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        summary = {}
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            row = result.result.data_array[0]
            summary = dict(zip(columns, row))
            total = int(summary.get("total_encounters", 0) or 0)
            readmits = int(summary.get("readmissions", 0) or 0)
            summary["readmission_rate"] = round(readmits / total * 100, 1) if total > 0 else 0
        trends = {}
        try:
            tr = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=trend_query, wait_timeout="30s")
            if tr.result and tr.result.data_array:
                cols = [c.name for c in tr.manifest.schema.columns]
                td = dict(zip(cols, tr.result.data_array[0]))
                tw_los = float(td.get("this_week_los") or 0)
                lw_los = float(td.get("last_week_los") or 0)
                tw_re = int(td.get("this_week_readmits") or 0)
                lw_re = int(td.get("last_week_readmits") or 0)
                tw_enc = int(td.get("this_week_enc") or 0)
                lw_enc = int(td.get("last_week_enc") or 0)
                if lw_los > 0:
                    trends["los_trend"] = round((tw_los - lw_los) / lw_los * 100, 1)
                if lw_re > 0:
                    trends["readmit_trend"] = round((tw_re - lw_re) / lw_re * 100, 1)
                if lw_enc > 0:
                    trends["enc_trend"] = round((tw_enc - lw_enc) / lw_enc * 100, 1)
        except Exception:
            pass
        summary["trends"] = trends
        return jsonify({"summary": summary})
    except Exception as e:
        return jsonify({"error": str(e), "summary": {}}), 200


@app.route("/api/encounters/by-hospital", methods=["GET"])
def get_encounters_by_hospital():
    try:
        w = get_workspace_client()
        query = f"""SELECT hospital, COUNT(*) as encounter_count,
               SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) as readmission_count,
               ROUND(AVG(los_days), 1) as avg_los
        FROM {ENCOUNTERS_TABLE} GROUP BY hospital ORDER BY hospital"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        data = []
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            for row in result.result.data_array:
                data.append(dict(zip(columns, row)))
        return jsonify({"hospital_stats": data})
    except Exception as e:
        return jsonify({"error": str(e), "hospital_stats": []}), 200


@app.route("/api/encounters/by-department", methods=["GET"])
def get_encounters_by_department():
    try:
        w = get_workspace_client()
        query = f"""SELECT department, COUNT(*) as encounter_count,
               SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) as readmission_count,
               ROUND(AVG(los_days), 1) as avg_los
        FROM {ENCOUNTERS_TABLE} GROUP BY department ORDER BY encounter_count DESC"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        data = []
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            for row in result.result.data_array:
                data.append(dict(zip(columns, row)))
        return jsonify({"department_stats": data})
    except Exception as e:
        return jsonify({"error": str(e), "department_stats": []}), 200


@app.route("/api/encounters/timeline", methods=["GET"])
def get_encounters_timeline():
    try:
        w = get_workspace_client()
        days = request.args.get("days", 30, type=int)
        query = f"""SELECT DATE(admit_date) as encounter_date, COUNT(*) as encounter_count,
               SUM(CASE WHEN is_readmission THEN 1 ELSE 0 END) as readmission_count
        FROM {ENCOUNTERS_TABLE} WHERE admit_date >= CURRENT_DATE - INTERVAL {days} DAYS
        GROUP BY DATE(admit_date) ORDER BY encounter_date ASC"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        timeline = []
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            for row in result.result.data_array:
                data = dict(zip(columns, row))
                timeline.append({"date": str(data.get("encounter_date")),
                                 "encounters": int(data.get("encounter_count", 0) or 0),
                                 "readmissions": int(data.get("readmission_count", 0) or 0)})
        return jsonify({"timeline": timeline, "days": days})
    except Exception as e:
        return jsonify({"error": str(e), "timeline": []}), 200


@app.route("/api/encounters/readmissions", methods=["GET"])
def get_readmissions():
    try:
        w = get_workspace_client()
        limit = request.args.get("limit", 5, type=int)
        query = f"""SELECT e.encounter_id, e.hospital, e.department, e.los_days,
            e.admit_date, e.discharge_date, e.payer,
            COALESCE(d.total_drug_cost, 0) as total_drug_cost
        FROM {ENCOUNTERS_TABLE} e
        LEFT JOIN (
            SELECT encounter_id, ROUND(SUM(total_cost), 2) as total_drug_cost
            FROM {DRUG_COSTS_TABLE} GROUP BY encounter_id
        ) d ON e.encounter_id = d.encounter_id
        WHERE e.is_readmission = true
        ORDER BY e.admit_date DESC LIMIT {limit}"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        readmissions = []
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            for row in result.result.data_array:
                data = dict(zip(columns, row))
                readmissions.append({
                    "encounter_id": data.get("encounter_id"), "hospital": data.get("hospital"),
                    "department": data.get("department"), "los_days": data.get("los_days"),
                    "admit_date": str(data.get("admit_date")),
                    "discharge_date": str(data.get("discharge_date", "")),
                    "payer": data.get("payer"),
                    "total_drug_cost": float(data.get("total_drug_cost") or 0),
                })
        return jsonify({"readmissions": readmissions, "count": len(readmissions)})
    except Exception as e:
        return jsonify({"error": str(e), "readmissions": []}), 200


@app.route("/api/recommendations/latest", methods=["GET"])
def get_recommendations():
    try:
        w = get_workspace_client()
        limit = request.args.get("limit", 5, type=int)
        query = f"""SELECT id, encounter_id, analysis_type, insights, recommendations, created_at
        FROM {ANALYSIS_TABLE} WHERE recommendations IS NOT NULL AND recommendations != ''
        ORDER BY created_at DESC LIMIT {limit}"""
        result = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=query, wait_timeout="30s")
        recommendations = []
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            for row in result.result.data_array:
                data = dict(zip(columns, row))
                recommendations.append({
                    "id": data.get("id"), "encounter_id": data.get("encounter_id"),
                    "type": data.get("analysis_type"),
                    "insight": (data.get("insights", "")[:200] + "...") if len(data.get("insights", "")) > 200 else data.get("insights", ""),
                    "action": data.get("recommendations"), "created_at": data.get("created_at")
                })
        return jsonify({"recommendations": recommendations, "count": len(recommendations)})
    except Exception as e:
        return jsonify({"error": str(e), "recommendations": []}), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
