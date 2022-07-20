-- Databricks notebook source
-- DROP TABLE sandbox.autodriller_dev_control_table;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.autodriller_dev_control_table (
rest_service STRING,
ingestion_starttimestamp STRING
)
PARTITIONED BY (asset_id string)
LOCATION "/mnt/auto_driller/dev/control_table/"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.autodriller_dev_assets (
assest_id STRING
)
LOCATION "/mnt/auto_driller/dev/assets/"

-- COMMAND ----------

show create table sandbox.autodriller_control_table;

-- COMMAND ----------

-- insert into sandbox.autodriller_dev_assets values('22061363');
-- insert into sandbox.autodriller_dev_assets values('65642229');


-- COMMAND ----------

show create table sandbox.autodriller_dev_control_table;

-- COMMAND ----------

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- COMMAND ----------

insert into sandbox.autodriller_dev_control_table values('operations','0','22061363');
insert into sandbox.autodriller_dev_control_table values('operations','0','65642229');

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select ingestion_starttimestamp from sandbox.autodriller_dev_control_table where rest_service = 'operations' and asset_id=22061363
