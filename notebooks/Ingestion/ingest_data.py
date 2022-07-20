# Databricks notebook source
# MAGIC %run ../util/autodriller_util

# COMMAND ----------

dbutils.widgets.text("asset_id","")
dbutils.widgets.text("rest_operation","")

asset_id = dbutils.widgets.get("asset_id")
rest_operation = dbutils.widgets.get("rest_operation")

# COMMAND ----------

# MAGIC %md # Get last processed timestamp from control table

# COMMAND ----------

from datetime import datetime

def timenow():
  return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def getLastIngestionTime(asset_id,rest_operation):
  query = f""" select ingestion_starttimestamp from sandbox.autodriller_dev_control_table where rest_service = '{rest_operation}' and asset_id='{asset_id}' """
  df = spark.sql(query)
  timestamp = [r[0] for r in df.select("ingestion_starttimestamp").collect()]
  
  return timestamp

def updateIngestionTime(asset_id,rest_operation):
  current_time = timenow()
  query = f""" update table sandbox.autodriller_dev_control_table set ingestion_starttimestamp={current_time} where  rest_service = '{rest_operation}' and asset_id='{asset_id}' """
  df = spark.sql(query)


  

# COMMAND ----------

timestamp = getLastIngestionTime(asset_id,rest_operation)
url = corva_api[rest_operation]
path = raw_file_path.format(data_type=rest_operation,asset=str(asset_id))
today = str(date.today().strftime('%Y%m%d'))
file_path = path+today+"/"
if(int(asset_id)):
  writeRawData(int(asset_id),url,0,file_path)
#   updateIngestionTime(asset_id,rest_operation)


# COMMAND ----------

# MAGIC %md # How to use the code
# MAGIC 
# MAGIC ## DDL - folder has the control table
