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

# MAGIC %md load gold tables

# COMMAND ----------

df_1s_gold = spark.table('sandbox.auto_driller_ad_dashboard_1s_golden')
df_health_gold = spark.table('sandbox.auto_driller_ad_dashboard_health')

# COMMAND ----------

well_name_const = 'Keydets-A 47 #4H'
df_1s_gold = df_1s_gold.filter(col('well_name')!=well_name_const)
df_health_gold = df_health_gold.filter(col('well_name')!=well_name_const)

# COMMAND ----------

df_1s = spark.table('sandbox.auto_driller_1s_silver_test')

# COMMAND ----------

df_1s.select('well_name').distinct().show()

# COMMAND ----------

df_health = spark.table('sandbox.auto_driller_inference_mvp_health')

# COMMAND ----------

display(df_health)

# COMMAND ----------

#Add Constant columns to avoid information relink in dashboard
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
df_health = df_health.drop('rop__sum_values',
 'rop__median',
 'rop__mean',
 'rop__length',
 'rop__standard_deviation',
 'rop__variance',
 'rop__root_mean_square',
 'rop__maximum',
 'rop__absolute_maximum',
 'rop__minimum', 'prediction', 'AD_Health')
df_health = df_health.withColumnRenamed('AD_Health_Final', 'health_total')
df_health=df_health.withColumn("stand_id",df_health.stand_id.cast(IntegerType()))
const_columns = ['outlier_percent_rop',
 'health_rop',
 'outlier_percent_weight_on_bit',
 'health_weight_on_bit',
 'outlier_percent_diff_press',
 'health_diff_press',
 'outlier_percent_rotary_rpm',
 'health_rotary_rpm',
 'outlier_percent_rotary_torque',
 'health_rotary_torque']
for column in const_columns:
  df_health = df_health.withColumn(column, lit(999))

# COMMAND ----------

display(df_health)

# COMMAND ----------

df_health = df_health.union(df_health_gold)

# COMMAND ----------

df_1s = df_1s.union(df_1s_gold)

# COMMAND ----------

exportDFToSQLDB(df_1s, 'auto_driller_1s', overwrite=True)

# COMMAND ----------

exportDFToSQLDB(df_health, 'auto_driller_mvp_fools_gold', overwrite=True)
