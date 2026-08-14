# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA gold;
# MAGIC
# MAGIC -- Aggregated win rate by champion and lane, for the most recent patch only.
# MAGIC -- Also splits win rate by game length (<=25 min vs >25 min) to capture
# MAGIC -- champions whose power spikes early or late.
# MAGIC CREATE OR REPLACE TABLE winrate_by_champion_lane AS
# MAGIC WITH matches_with_patch AS (
# MAGIC     -- Derive the patch from game_version
# MAGIC     SELECT
# MAGIC         match_id,
# MAGIC         game_duration_minutes,
# MAGIC         concat(split(game_version, '\\.')[0], '.', split(game_version, '\\.')[1]) AS patch,
# MAGIC         CAST(split(game_version, '\\.')[0] AS INT) AS patch_major,
# MAGIC         CAST(split(game_version, '\\.')[1] AS INT) AS patch_minor
# MAGIC     FROM workspace.silver.general_info
# MAGIC ),
# MAGIC latest_patch AS (
# MAGIC     -- Always the most recent patch present in the data
# MAGIC     SELECT patch FROM matches_with_patch
# MAGIC     ORDER BY patch_major DESC, patch_minor DESC
# MAGIC     LIMIT 1
# MAGIC )
# MAGIC SELECT
# MAGIC     p.champion,
# MAGIC     p.lane_position,
# MAGIC     COUNT(*) AS games,
# MAGIC     ROUND(SUM(INT(p.win))/COUNT(*), 3) as winrate,
# MAGIC     ROUND(SUM(CASE WHEN g.game_duration_minutes <= 25 THEN INT(p.win) END)/COUNT(CASE WHEN g.game_duration_minutes <= 25 THEN 1 END), 3) AS early_game_wr,
# MAGIC     ROUND(SUM(CASE WHEN g.game_duration_minutes > 25 THEN INT(p.win) END)/COUNT(CASE WHEN g.game_duration_minutes > 25 THEN 1 END), 3) AS late_game_wr,
# MAGIC     g.patch
# MAGIC FROM workspace.silver.player_info as p
# MAGIC INNER JOIN matches_with_patch as g ON p.match_id = g.match_id
# MAGIC WHERE g.patch = (SELECT patch FROM latest_patch)
# MAGIC GROUP BY p.champion, p.lane_position, g.patch
# MAGIC -- Minimum sample size to avoid noisy win rates from too few games
# MAGIC HAVING COUNT(*) > 500
# MAGIC ORDER BY p.lane_position, winrate DESC;