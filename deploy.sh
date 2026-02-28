#!/bin/bash
set -e

TARGET="${1:-dev}"
PROFILE="${2:-}"
APP_NAME="${TARGET}-med-logistics-nba"

PROFILE_ARG=""
[[ -n "$PROFILE" ]] && PROFILE_ARG="-p $PROFILE"

echo "=== Deploying Medical Logistics NBA App (target: $TARGET) ==="
echo ""

# Step 1: Deploy bundle
echo "[1/5] Deploying bundle..."
databricks bundle deploy -t "$TARGET" $PROFILE_ARG || { echo "FAILED: bundle deploy"; exit 1; }
echo "  Bundle deployed successfully"

# Step 2: Get app service principal using jq for reliable parsing
echo ""
echo "[2/5] Getting app service principal..."
SP_INFO=$(databricks apps get "$APP_NAME" $PROFILE_ARG --output json 2>/dev/null || echo "{}")

# Use jq for reliable JSON parsing (try name first, then id)
if command -v jq &> /dev/null; then
    SP_NAME=$(echo "$SP_INFO" | jq -r '.service_principal_name // empty' 2>/dev/null || echo "")
    SP_ID=$(echo "$SP_INFO" | jq -r '.service_principal_id // empty' 2>/dev/null || echo "")
else
    # Fallback to grep if jq not available
    SP_NAME=$(echo "$SP_INFO" | grep -o '"service_principal_name"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4 || echo "")
    SP_ID=$(echo "$SP_INFO" | grep -o '"service_principal_id"[[:space:]]*:[[:space:]]*"[^"]*"' | cut -d'"' -f4 || echo "")
fi

if [[ -z "$SP_NAME" && -z "$SP_ID" ]]; then
    echo "  WARNING: Could not get service principal info."
    echo "  App may not be fully deployed yet. Skipping permission grants."
    echo "  Run grant_permissions manually after app deploys."
else
    echo "  App: $APP_NAME"
    echo "  Service Principal Name: ${SP_NAME:-N/A}"
    echo "  Service Principal ID: ${SP_ID:-N/A}"
    
    # Step 3: Grant permissions (passing name first, then id)
    echo ""
    echo "[3/5] Granting permissions to service principal..."
    databricks bundle run grant_permissions -t "$TARGET" $PROFILE_ARG \
        --notebook-params "var.service_principal_name=$SP_NAME,var.service_principal_id=$SP_ID" \
        || { echo "FAILED: grant_permissions"; exit 1; }
    echo "  Permissions granted successfully"
fi

# Step 4: Setup vector search
echo ""
echo "[4/5] Setting up vector search..."
databricks bundle run setup_vector_search -t "$TARGET" $PROFILE_ARG || { echo "FAILED: setup_vector_search"; exit 1; }
echo "  Vector search setup complete"

# Step 5: Run diagnostics
echo ""
echo "[5/5] Running diagnostics..."
databricks bundle run diagnostic_check -t "$TARGET" $PROFILE_ARG || { echo "WARNING: diagnostic_check had issues"; }
echo "  Diagnostics complete"

echo ""
echo "=== SUCCESS ==="
echo "App URL: Check Databricks workspace for $APP_NAME"
echo ""
