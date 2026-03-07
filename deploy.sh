#!/bin/bash
set -e

TARGET="${1:-dev}"
PROFILE="${2:-}"
APP_NAME="${TARGET}-hospital-control-tower"

PROFILE_ARG=""
[[ -n "$PROFILE" ]] && PROFILE_ARG="-p $PROFILE"

echo "=== Deploying Hospital Control Tower (target: $TARGET) ==="
echo ""

# Step 1: Deploy bundle
echo "[1/6] Deploying bundle..."
databricks bundle deploy -t "$TARGET" $PROFILE_ARG || { echo "FAILED: bundle deploy"; exit 1; }
echo "  Bundle deployed successfully"

# Step 2: Create tables (must run before vector search so source tables exist)
echo ""
echo "[2/6] Setting up tables..."
databricks bundle run setup_lakebase -t "$TARGET" $PROFILE_ARG \
    || { echo "FAILED: setup_lakebase"; exit 1; }
echo "  Tables created successfully"

# Step 3: Setup vector search (creates endpoint + encounters index)
echo ""
echo "[3/6] Setting up vector search..."
databricks bundle run setup_vector_search -t "$TARGET" $PROFILE_ARG || { echo "FAILED: setup_vector_search"; exit 1; }
echo "  Vector search setup complete"

# Step 4: Setup SOP vector search (creates SOP index on same endpoint)
echo ""
echo "[4/6] Setting up SOP vector search..."
databricks bundle run setup_sop_vector_search -t "$TARGET" $PROFILE_ARG || { echo "WARNING: SOP vector search setup had issues (SOPs may not be loaded yet)"; }
echo "  SOP vector search setup complete"

# Step 5: Grant permissions (runs AFTER all resources exist)
echo ""
echo "[5/6] Granting permissions to app service principal..."
databricks bundle run grant_permissions -t "$TARGET" $PROFILE_ARG \
    || { echo "FAILED: grant_permissions"; exit 1; }
echo "  Permissions granted successfully"

# Step 6: Run diagnostics
echo ""
echo "[6/6] Running diagnostics..."
databricks bundle run diagnostic_check -t "$TARGET" $PROFILE_ARG || { echo "WARNING: diagnostic_check had issues"; }
echo "  Diagnostics complete"

echo ""
echo "=== SUCCESS ==="
echo "App URL: Check Databricks workspace for $APP_NAME"
echo ""
