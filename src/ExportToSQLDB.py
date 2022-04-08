# Databricks notebook source
def exportToSQLDB(delta_table_name, sql_db_table_name):
  sql_db_table_name = 'auto_driller_1s'
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
  df =spark.read.jdbc(url=url, table=SINK_DELTA_TABLE, properties=connectionProperties)
  return df
