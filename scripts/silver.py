# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA silver;
# MAGIC
# MAGIC --Create format table General_info
# MAGIC CREATE TABLE IF NOT EXISTS general_info
# MAGIC USING delta 
# MAGIC AS SELECT
# MAGIC     metadata.matchId as match_id,
# MAGIC     info.gameVersion as game_version,
# MAGIC     ROUND(info.gameDuration/60, 2) as game_duration_minutes
# MAGIC FROM workspace.bronze.b_matches
# MAGIC WHERE 1=0;
# MAGIC
# MAGIC --Merge new data without remakes into General_info
# MAGIC MERGE INTO general_info AS g
# MAGIC USING (
# MAGIC     SELECT metadata.matchId AS match_id,
# MAGIC     info.gameVersion AS game_version,
# MAGIC     ROUND(info.gameDuration/60, 2) AS game_duration_minutes
# MAGIC     FROM workspace.bronze.b_matches
# MAGIC     WHERE info.gameDuration > (14*60)) AS b
# MAGIC ON g.match_id = b.match_id
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Create format table Player_info
# MAGIC CREATE TABLE IF NOT EXISTS player_info
# MAGIC USING delta
# MAGIC AS SELECT
# MAGIC     match_id,
# MAGIC     player_name.riotIdGameName AS player_name,
# MAGIC     player_name.individualPosition AS lane_position,
# MAGIC     player_name.championName AS champion,
# MAGIC     player_name.kills AS kills,
# MAGIC     player_name.deaths AS deaths,
# MAGIC     player_name.assists AS assists,
# MAGIC     player_name.win AS win,
# MAGIC     player_name.goldEarned AS gold_earned,
# MAGIC     player_name.totalMinionsKilled AS minions_killed,
# MAGIC     player_name.totalDamageDealtToChampions AS damage_dealt_to_champions,    
# MAGIC     player_name.visionScore AS vision_score
# MAGIC FROM (
# MAGIC     SELECT 
# MAGIC         metadata.matchId AS match_id,
# MAGIC         EXPLODE(info.participants) AS player_name
# MAGIC     FROM workspace.bronze.b_matches
# MAGIC     WHERE 1=0
# MAGIC );
# MAGIC
# MAGIC --Merge new data without remakes into Player_info
# MAGIC MERGE INTO player_info AS p
# MAGIC USING (
# MAGIC     SELECT
# MAGIC     match_id,
# MAGIC     player_name.riotIdGameName AS player_name,
# MAGIC     player_name.individualPosition AS lane_position,
# MAGIC     CASE player_name.championName
# MAGIC         -- Champion with different Id Name
# MAGIC         WHEN 'MonkeyKing' THEN 'Wukong'
# MAGIC         WHEN 'Nunu'         THEN 'Nunu & Willump'
# MAGIC         WHEN 'Renata'       THEN 'Renata Glasc'
# MAGIC         -- Champions with apostrophes
# MAGIC         WHEN 'Chogath'      THEN 'Cho''Gath'
# MAGIC         WHEN 'Kaisa'        THEN 'Kai''Sa'
# MAGIC         WHEN 'Khazix'       THEN 'Kha''Zix'
# MAGIC         WHEN 'KogMaw'       THEN 'Kog''Maw'
# MAGIC         WHEN 'RekSai'       THEN 'Rek''Sai'
# MAGIC         WHEN 'Velkoz'       THEN 'Vel''Koz'
# MAGIC         WHEN 'Belveth'      THEN 'Bel''Veth'
# MAGIC         WHEN 'KSante'       THEN 'K''Sante'
# MAGIC         -- Champions with spaces or puntuaction
# MAGIC         WHEN 'DrMundo'      THEN 'Dr. Mundo'
# MAGIC         WHEN 'JarvanIV'     THEN 'Jarvan IV'
# MAGIC         WHEN 'MasterYi'     THEN 'Master Yi'
# MAGIC         WHEN 'MissFortune'  THEN 'Miss Fortune'
# MAGIC         WHEN 'TahmKench'    THEN 'Tahm Kench'
# MAGIC         WHEN 'TwistedFate'  THEN 'Twisted Fate'
# MAGIC         WHEN 'XinZhao'      THEN 'Xin Zhao'
# MAGIC         -- Champions with format difference
# MAGIC         WHEN 'FiddleSticks'  THEN 'Fiddlesticks'
# MAGIC         ELSE player_name.championName
# MAGIC         END AS champion,
# MAGIC     player_name.kills AS kills,
# MAGIC     player_name.deaths AS deaths,
# MAGIC     player_name.assists AS assists,
# MAGIC     player_name.win AS win,
# MAGIC     player_name.goldEarned AS gold_earned,
# MAGIC     player_name.totalMinionsKilled AS minions_killed,
# MAGIC     player_name.totalDamageDealtToChampions AS damage_dealt_to_champions,    
# MAGIC     player_name.visionScore AS vision_score
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC         metadata.matchId AS match_id,
# MAGIC         EXPLODE(info.participants) AS player_name
# MAGIC         FROM workspace.bronze.b_matches
# MAGIC         WHERE info.gameDuration > (14*60)
# MAGIC         )
# MAGIC     ) AS b
# MAGIC ON p.match_id = b.match_id
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checking the last 2 patches and deleting the previous ones
# MAGIC
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA silver;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW silver_patches_to_keep AS
# MAGIC SELECT patch FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         concat(split(game_version, '\\.')[0], '.', split(game_version, '\\.')[1]) AS patch,
# MAGIC         CAST(split(game_version, '\\.')[0] AS INT) AS major,
# MAGIC         CAST(split(game_version, '\\.')[1] AS INT) AS minor
# MAGIC     FROM general_info
# MAGIC )
# MAGIC ORDER BY major DESC, minor DESC
# MAGIC LIMIT 2;  -- number of patches to keep, same as sync_to_databricks.py, same as bronze_layer
# MAGIC
# MAGIC -- Deleting previous info of game and players of that game
# MAGIC DELETE FROM general_info
# MAGIC WHERE concat(split(game_version, '\\.')[0], '.', split(game_version, '\\.')[1])
# MAGIC       NOT IN (SELECT patch FROM silver_patches_to_keep);
# MAGIC
# MAGIC DELETE FROM player_info
# MAGIC WHERE match_id NOT IN (SELECT match_id FROM general_info);
# MAGIC
# MAGIC VACUUM general_info;
# MAGIC VACUUM player_info;