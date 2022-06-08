# Databricks notebook source
def exportToSQLDB(delta_table_name, sql_db_table_name):
  serverName = "jdbc:sqlserver://sqls-wells-ussc-prd.database.windows.net:1433"
  databaseName = "WellsIntelGoldTableDev"
  url = serverName + ";" + "databaseName=" + databaseName + ";"

  databricksKeyVaultScope = "Wells.Databricks.Keyvault.Secrets"

  userName = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksUsername")
  password = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksPassword")

  # #write to Azure SQL    
  df = spark.table(delta_table_name)
  try:
    df.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
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


def exportDFToSQLDB(df, sql_db_table_name):
  #sql_db_table_name = 'auto_driller_1s'
  serverName = "jdbc:sqlserver://sqls-wells-ussc-prd.database.windows.net:1433"
  databaseName = "WellsIntelGoldTableDev"
  url = serverName + ";" + "databaseName=" + databaseName + ";"

  databricksKeyVaultScope = "Wells.Databricks.Keyvault.Secrets"

  userName = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksUsername")
  password = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksPassword")

  # #write to Azure SQL    
  #df = spark.table(delta_table_name)
  try:
    df.write \
        .format("com.microsoft.sqlserver.jdbc.spark") \
        .mode("overwrite") \
        .option("url", url) \
        .option("dbtable", sql_db_table_name) \
        .option("user", userName) \
        .option("password", password) \
        .save()
  except ValueError as error :
      print("Connector write failed", error)

# COMMAND ----------

exportToSQLDB('sandbox.auto_driller_1s_silver', 'auto_driller_1s')

# COMMAND ----------

df_health = loadFromSQLDB('auto_driller_mvp_fools_gold')

# COMMAND ----------

df_health = df_health.toPandas()

# COMMAND ----------

thred1, thred2 = df_health['stand_duration'].quantile([0.01, 0.97]).to_list()
print(f'thre1={thred1}, thre2={thred2}')

# COMMAND ----------

df_health.loc[df_health['stand_duration']>thred2, 'health_total']='bad'

# COMMAND ----------

df_health[(df_health['stand_duration']>thred2)][['well_name', 'stand_id', 'health_total']]

# COMMAND ----------

sparkDF=spark.createDataFrame(df_health) 


# COMMAND ----------

exportDFToSQLDB(sparkDF, 'auto_driller_mvp_fools_gold')

# COMMAND ----------

df_1s = spark.table('sandbox.auto_driller_1s_silver')

# COMMAND ----------

df_1s = df_1s.toPandas()

# COMMAND ----------

df_1s.head()

# COMMAND ----------

import pandas as pd
min_time = df_1s.groupby(['asset_id', 'stand_id'])['RecordDateTime'].transform('min') + pd.Timedelta(5, unit = 'm')
max_time = df_1s.groupby(['asset_id', 'stand_id'])['RecordDateTime'].transform('max') - pd.Timedelta(5, unit = 'm')
mask_time = ((df_1s['RecordDateTime']>min_time) & (df_1s['RecordDateTime'] < max_time))
df_trunc = df_1s[mask_time]

# COMMAND ----------

sparkDF_1s=spark.createDataFrame(df_trunc) 

# COMMAND ----------

exportDFToSQLDB(sparkDF_1s, 'auto_driller_1s')

# COMMAND ----------


