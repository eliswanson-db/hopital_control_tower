# Databricks notebook source
# MAGIC %md
# MAGIC # Grant Permissions to App Service Principal
# MAGIC 
# MAGIC Grants permissions for the Medical Logistics NBA app to access all data tables, models, and vector search.

# COMMAND ----------

# Configuration
CATALOG = spark.conf.get("var.catalog", "eswanson_demo")
SCHEMA = spark.conf.get("var.schema", "med_logistics_nba")
APP_NAME = spark.conf.get("var.app_name", "dev-med-logistics-nba")
SP_ID = spark.conf.get("var.service_principal_id", "")
SP_NAME = spark.conf.get("var.service_principal_name", "")

print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"App Name: {APP_NAME}")
print(f"Service Principal ID: {SP_ID}")
print(f"Service Principal Name: {SP_NAME}")

# COMMAND ----------

# Determine the principal to grant to - try name first, then ID
if SP_NAME:
    PRINCIPAL = f"`{SP_NAME}`"
    print(f"Using service principal name: {PRINCIPAL}")
elif SP_ID:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    try:
        sp = w.service_principals.get(SP_ID)
        PRINCIPAL = f"`{sp.display_name}`"
        print(f"Looked up service principal: {PRINCIPAL}")
    except Exception as e:
        print(f"Could not look up SP by ID: {e}")
        PRINCIPAL = f"`{APP_NAME}`"
        print(f"Falling back to app name: {PRINCIPAL}")
else:
    PRINCIPAL = f"`{APP_NAME}`"
    print(f"Using app name as principal: {PRINCIPAL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Grant Catalog and Schema Access

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

# Grant USE CATALOG
try:
    spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO {PRINCIPAL}")
    print(f"Granted USE CATALOG on {CATALOG}")
except Exception as e:
    print(f"USE CATALOG: {e}")

# Grant USE SCHEMA
try:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO {PRINCIPAL}")
    print(f"Granted USE SCHEMA on {CATALOG}.{SCHEMA}")
except Exception as e:
    print(f"USE SCHEMA: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Grant Table Permissions (All Data Tables + Output Tables)

# COMMAND ----------

# All tables the app needs to READ (medical logistics model)
READ_TABLES = [
    # Core encounter and fact tables
    "dim_encounters",
    "fact_drug_costs",
    "fact_staffing",
    "fact_ed_wait_times",
    "fact_operational_kpis",
    "hospital_overview",  # View (derived from above)
    # Embedding tables for vector search
    "encounters_for_embedding",
    # SOP tables for RAG
    "sop_pdfs",
    "sop_parsed",
    "sop_chunks",
]

# Tables the app needs to WRITE (MODIFY)
WRITE_TABLES = [
    "analysis_outputs",
]

# Grant SELECT on read tables
print("\nGranting SELECT permissions:")
for table in READ_TABLES:
    try:
        spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.{table} TO {PRINCIPAL}")
        print(f"  SELECT on {table}")
    except Exception as e:
        print(f"  SELECT on {table}: {e}")

# Grant SELECT and MODIFY on write tables
print("\nGranting SELECT, MODIFY permissions:")
for table in WRITE_TABLES:
    try:
        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {CATALOG}.{SCHEMA}.{table} TO {PRINCIPAL}")
        print(f"  SELECT, MODIFY on {table}")
    except Exception as e:
        print(f"  MODIFY on {table}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Grant Vector Search Permissions

# COMMAND ----------

VECTOR_ENDPOINT = f"{CATALOG}_{SCHEMA}_vector_endpoint".replace("-", "_")
ENCOUNTER_VECTOR_INDEX = f"{CATALOG}.{SCHEMA}.encounters_vector_index"
SOP_VECTOR_INDEX = f"{CATALOG}.{SCHEMA}.sop_vector_index"

print(f"Vector Endpoint: {VECTOR_ENDPOINT}")
print(f"Encounter Vector Index: {ENCOUNTER_VECTOR_INDEX}")
print(f"SOP Vector Index: {SOP_VECTOR_INDEX}")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Grant access to vector search endpoint
try:
    from databricks.sdk.service.vectorsearch import EndpointAccessControlRequest, EndpointPermissionLevel
    w.vector_search_endpoints.set_permissions(
        vector_search_endpoint_id=VECTOR_ENDPOINT,
        access_control_list=[
            EndpointAccessControlRequest(
                service_principal_name=SP_NAME or APP_NAME,
                permission_level=EndpointPermissionLevel.CAN_USE
            )
        ]
    )
    print(f"Granted CAN_USE on vector endpoint {VECTOR_ENDPOINT}")
except Exception as e:
    print(f"Vector endpoint permission: {e}")

print(f"Vector index permissions inherited from source tables:")
print(f"  - {ENCOUNTER_VECTOR_INDEX} (from encounters_for_embedding)")
print(f"  - {SOP_VECTOR_INDEX} (from sop_chunks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Grant Foundation Model Access

# COMMAND ----------

MODEL_ENDPOINTS = [
    "databricks-claude-sonnet-4-5",
]

for endpoint_name in MODEL_ENDPOINTS:
    try:
        from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel
        w.serving_endpoints.set_permissions(
            serving_endpoint_id=endpoint_name,
            access_control_list=[
                ServingEndpointAccessControlRequest(
                    service_principal_name=SP_NAME or APP_NAME,
                    permission_level=ServingEndpointPermissionLevel.CAN_QUERY
                )
            ]
        )
        print(f"Granted CAN_QUERY on {endpoint_name}")
    except Exception as e:
        print(f"Model {endpoint_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("PERMISSIONS SUMMARY")
print("=" * 60)
print(f"Service Principal: {SP_NAME or APP_NAME}")
print(f"Service Principal ID: {SP_ID or 'N/A'}")
print(f"")
print("Data Access:")
print(f"  - USE CATALOG on {CATALOG}")
print(f"  - USE SCHEMA on {CATALOG}.{SCHEMA}")
print(f"  - SELECT on: {', '.join(READ_TABLES)}")
print(f"  - SELECT, MODIFY on: {', '.join(WRITE_TABLES)}")
print(f"")
print("Vector Search:")
print(f"  - CAN_USE on {VECTOR_ENDPOINT}")
print(f"  - Encounter Index: {ENCOUNTER_VECTOR_INDEX} (inherited from encounters_for_embedding)")
print(f"  - SOP Index: {SOP_VECTOR_INDEX} (inherited from sop_chunks)")
print(f"")
print("Models:")
print(f"  - CAN_QUERY on: {', '.join(MODEL_ENDPOINTS)}")
print("=" * 60)
