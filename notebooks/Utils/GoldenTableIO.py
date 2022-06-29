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

df_1s = loadFromSQLDB('auto_driller_1s')
df_health = loadFromSQLDB('auto_driller_mvp_fools_gold')

# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'auto_driller_AD_dashboard_1s_golden'
write_table_location = 'sandbox'
mnt_location = f'/mnt/delta/{write_table_name}/'


def createDeltaTable(df):
  print('start writing to dbfs')
  df.write.format(write_format).mode(write_mode).option("overwriteSchema", "true").save(mnt_location)
  print('finish writing to dbfs')
  try:
      drop_table_command =f'DROP TABLE {write_table_location}.{write_table_name}'
      spark.sql(drop_table_command)
  except:
      print('Table not exist')
  print('Start creating the table')
  create_table_command = f'''
                          CREATE TABLE {write_table_location}.{write_table_name}
                          USING DELTA LOCATION '{mnt_location}'
                          '''
  spark.sql(create_table_command)
  print('Finish creating the table')

createDeltaTable(df_1s)

# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'auto_driller_AD_dashboard_health'
write_table_location = 'sandbox'
mnt_location = f'/mnt/delta/{write_table_name}/'


def createDeltaTable(df):
  print('start writing to dbfs')
  df.write.format(write_format).mode(write_mode).option("overwriteSchema", "true").save(mnt_location)
  print('finish writing to dbfs')
  try:
      drop_table_command =f'DROP TABLE {write_table_location}.{write_table_name}'
      spark.sql(drop_table_command)
  except:
      print('Table not exist')
  print('Start creating the table')
  create_table_command = f'''
                          CREATE TABLE {write_table_location}.{write_table_name}
                          USING DELTA LOCATION '{mnt_location}'
                          '''
  spark.sql(create_table_command)
  print('Finish creating the table')
  
createDeltaTable(df_health)
