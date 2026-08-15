# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC -- create checkpoint for Auto Loader
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.bronze.pipeline_checkpoints;

# COMMAND ----------

#Load path of json files organized by patch and checkpoint of read files
checkpoint_base = "/Volumes/workspace/bronze/pipeline_checkpoints"
path_json_matches = "/Volumes/workspace/bronze/raw_data/*/matches/"
path_json_timelines = "/Volumes/workspace/bronze/raw_data/*/timelines/"

# Auto Loader for matches and timelines: read only files that have never been read
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/matches_schema")
    .load(path_json_matches)
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_base}/matches_checkpoint")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.bronze.b_matches")
).awaitTermination()

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/timelines_schema")
    .load(path_json_timelines)
    .writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_base}/timelines_checkpoint")
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.bronze.b_timelines")
).awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking the last 2 patches and deleting the previous ones
# MAGIC
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA bronze;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW patches_to_keep AS
# MAGIC SELECT patch FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         concat(split(info.gameVersion, '\\.')[0], '.', split(info.gameVersion, '\\.')[1]) AS patch,
# MAGIC         CAST(split(info.gameVersion, '\\.')[0] AS INT) AS major,
# MAGIC         CAST(split(info.gameVersion, '\\.')[1] AS INT) AS minor
# MAGIC     FROM b_matches
# MAGIC )
# MAGIC ORDER BY major DESC, minor DESC
# MAGIC LIMIT 2; -- number of patches to keep, same as sync_to_databricks.py
# MAGIC
# MAGIC -- Deleting old matches
# MAGIC DELETE FROM b_matches
# MAGIC WHERE concat(split(info.gameVersion, '\\.')[0], '.', split(info.gameVersion, '\\.')[1])
# MAGIC       NOT IN (SELECT patch FROM patches_to_keep);
# MAGIC
# MAGIC -- Deleting the linked timelines
# MAGIC DELETE FROM b_timelines
# MAGIC WHERE metadata.matchId NOT IN (SELECT metadata.matchId FROM b_matches);
# MAGIC
# MAGIC -- Vacuum space of previous deleted data
# MAGIC VACUUM b_matches;
# MAGIC VACUUM b_timelines;