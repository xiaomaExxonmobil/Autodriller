# Databricks notebook source
from pyspark.sql.functions import col
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as f
from pyspark.sql import Window


def exportToSQLDB(delta_table_name, sql_db_table_name, overwrite=False):
  serverName = "jdbc:sqlserver://sqls-wells-ussc-prd.database.windows.net:1433"
  databaseName = "WellsIntelGoldTableDev"
  url = serverName + ";" + "databaseName=" + databaseName + ";"

  databricksKeyVaultScope = "Wells.Databricks.Keyvault.Secrets"

  userName = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksUsername")
  password = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksPassword")
  write_mode = 'overwrite' if overwrite else 'append'
  # #write to Azure SQL    
  df = spark.table(delta_table_name)
  try:
    df.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode(write_mode) \
        .option("url", url) \
        .option("dbtable", sql_db_table_name) \
        .option("user", userName) \
        .option("password", password) \
        .save()
  except ValueError as error :
      print("Connector write failed", error)
      
      
def loadFromSQLDB(sql_db_table_name):
  databricksKeyVaultScope = "Wells.Databricks.Keyvault.Secrets"
  userName = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksUsername")
  password = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksPassword")
  connectionProperties = {
    "user" : userName,
    "password" : password,
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  }
  serverName = "jdbc:sqlserver://sqls-wells-ussc-prd.database.windows.net:1433"
  databaseName = "WellsIntelGoldTableDev"
  url = serverName + ";" + "databaseName=" + databaseName + ";"
  df =spark.read.jdbc(url=url, table=sql_db_table_name, properties=connectionProperties)
  return df


def exportDFToSQLDB(df, sql_db_table_name, overwrite=False ):
  serverName = "jdbc:sqlserver://sqls-wells-ussc-prd.database.windows.net:1433"
  databaseName = "WellsIntelGoldTableDev"
  url = serverName + ";" + "databaseName=" + databaseName + ";"

  databricksKeyVaultScope = "Wells.Databricks.Keyvault.Secrets"

  userName = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksUsername")
  password = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksPassword")
  write_mode = 'overwrite' if overwrite else 'append'

  try:
    df.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode(write_mode) \
        .option("url", url) \
        .option("dbtable", sql_db_table_name) \
        .option("user", userName) \
        .option("password", password) \
        .save()
  except ValueError as error :
      print("Connector write failed", error)

# COMMAND ----------

df_1s = spark.table('sandbox.auto_driller_1s_silver_test')
w = Window.partitionBy(['asset_id', 'stand_id'])
df_temp=df_1s.withColumn('max_time', f.max('RecordDateTime').over(w) - f.expr('INTERVAL 5 MINUTES'))
df_temp=df_temp.withColumn('min_time', f.min('RecordDateTime').over(w) + f.expr('INTERVAL 5 MINUTES'))
df_temp = df_temp.filter((col('RecordDateTime')>col('min_time'))&(col('RecordDateTime')<col('max_time'))).drop('max_time', 'min_time')

# COMMAND ----------

exportDFToSQLDB(df_temp, 'auto_driller_1s', overwrite=False)

# COMMAND ----------

exportToSQLDB('sandbox.auto_driller_mvp_health_test', 'auto_driller_mvp_fools_gold', overwrite=False)

# COMMAND ----------

df_new = spark.table('sandbox.auto_driller_mvp_health_test')

# COMMAND ----------

display(df_new)

# COMMAND ----------

import pyspark.sql.functions as func
df_agg = df_new.groupBy('asset_id').agg(func.first('well_name'), func.max('stand_id'))
display(df_agg)

# COMMAND ----------


