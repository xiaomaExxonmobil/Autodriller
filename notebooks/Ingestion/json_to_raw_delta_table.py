# Databricks notebook source
# MAGIC %run ../util/Constants

# COMMAND ----------

dbutils.widgets.text("asset_id","")
dbutils.widgets.text("rest_operation","")

asset_id = dbutils.widgets.get("asset_id")
rest_operation = dbutils.widgets.get("rest_operation")

# COMMAND ----------

#create json dataframe
folder_path = raw_file_path.format(data_type=rest_operation,asset=str(asset_id))
print(folder_path)
# df = spark.read.format("json").load(folder_path)

# COMMAND ----------

dbutils.fs.ls("/dbfs/mnt/auto_driller/dev/wits_stage/65642229/")

# COMMAND ----------


