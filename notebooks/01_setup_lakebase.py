# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Tables for Medical Logistics NBA App
# MAGIC
# MAGIC Creates the analysis_outputs and poetry tables in Unity Catalog.

# COMMAND ----------

# Configuration
CATALOG = spark.conf.get("var.catalog", "eswanson_demo")
SCHEMA = spark.conf.get("var.schema", "med_logistics_nba")

print(f"Unity Catalog: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# Set the catalog
spark.sql(f"USE CATALOG {CATALOG}")
print(f"Using catalog: {CATALOG}")

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
print(f"Schema {CATALOG}.{SCHEMA} created/verified")

# COMMAND ----------

# Create analysis_outputs table
spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.analysis_outputs (
    id STRING COMMENT 'Unique analysis ID (UUID)',
    encounter_id STRING COMMENT 'Related encounter ID if applicable',
    analysis_type STRING NOT NULL COMMENT 'Type: readmission_prediction, root_cause_analysis, cost_optimization, etc.',
    insights STRING NOT NULL COMMENT 'Main analysis findings',
    recommendations STRING COMMENT 'Actionable recommendations',
    created_at TIMESTAMP COMMENT 'When analysis was created',
    agent_mode STRING NOT NULL COMMENT 'quick or deep',
    metadata MAP<STRING, STRING> COMMENT 'Additional metadata'
)
USING DELTA
COMMENT 'Agent analysis outputs for medical logistics next best action'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
"""
)

print(f"Table {CATALOG}.{SCHEMA}.analysis_outputs created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("SETUP COMPLETE")
print("=" * 60)
print(f"Tables created in {CATALOG}.{SCHEMA}:")
print("  - analysis_outputs: For agent insights and recommendations")
print("")
print("Next: Run 02_setup_vector_search.py to create embeddings")
print("=" * 60)

# COMMAND ----------

# Display created tables
display(spark.sql(f"SHOW TABLES IN {SCHEMA}"))
