# Databricks notebook source
import matplotlib.pyplot as plt
import pandas as pd
import pickle
from plotly.subplots import make_subplots
import plotly.express as px
from pyspark.sql import functions as func
from pyspark.sql.functions import col, lag, udf, stddev, min, max, first, last
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import seaborn as sns
import warnings
import json
import requests
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
warnings.filterwarnings("ignore", category=FutureWarning)
plt.style.use('seaborn')

# COMMAND ----------

# MAGIC %md ## Constants

# COMMAND ----------

PREFIX = '/dbfs'
AD_1S_STAGE =  "/AD_1s_stage/"
AD_OP_STAGE = "/AD_OP_stage/"
STAGE_DIR = PREFIX + AD_1S_STAGE
STAGE_OP_DIR = PREFIX + AD_OP_STAGE
LIMIT = 10000 #Corva API limit

# COMMAND ----------

# MAGIC %md ## Load the one second and operation tabel

# COMMAND ----------

df_1s = spark.table('sandbox.auto_driller_one_second')
df_op = spark.table('sandbox.auto_driller_op')

# COMMAND ----------

# MAGIC %md ## Data Ingestion through Corva API

# COMMAND ----------

dbutils.fs.rm(AD_1S_STAGE, recurse=True)
os.makedirs(STAGE_DIR, exist_ok=True)

# COMMAND ----------

def requests_retry_session(
    retries=3,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def getLatestTimeStamp(df, asset_id):
  return df_1s.filter(col('asset_id')==asset_id).select(max('timestamp')).collect()[0][0]  

def getWitsData(df, asset_id, is_new_well=False):
  apiKey = dbutils.secrets.get('Wells.Databricks.KeyVault.Secrets','corvaApi')
  url = 'https://data.corva.ai/api/v1/data/corva/wits/?'    
  start_timestamp = 0  if is_new_well else getLatestTimeStamp(df, asset_id)
  n = 1 
  while True:
    headers = {'authorization':apiKey}
    query = json.dumps({'asset_id': asset_id, 'timestamp':{"$gte":start_timestamp}})      
    s = requests.Session()
    response = requests_retry_session(session=s).get(url, headers=headers,
                                                     params = {"sort":json.dumps({'timestamp':1}),
                                                               'limit': LIMIT,
                                                               'query': query})
    if len(response.json())>2:
      resp = response.json()
      start_timestamp = resp[-1]['timestamp']
      #Save the file
      #os.makedirs(destFolder, exist_ok=True)
      filename = str(asset_id)+"_"+str(start_timestamp)+".json"
      destfile=STAGE_DIR+filename
      with open(destfile, 'w') as f:
        json.dump(resp, f)
      n += 1
      if n % 10 ==0: 
        print(f"Reach timestamp {start_timestamp} for asset {asset_id}")        
    else:
      print(f'Finish for asset={asset_id}')
      break
        
# H&P 546 547
asset_ids = [22061363, 65642229]
for asset_id in asset_ids:
  getWitsData(df_1s, asset_id)

# COMMAND ----------

# MAGIC %md ## Pull Operation Data

# COMMAND ----------

dbutils.fs.rm(AD_OP_STAGE, recurse=True)
os.makedirs(STAGE_OP_DIR, exist_ok=True)

def getOperations(df, asset_id, is_new_well=False):
    apiKey = dbutils.secrets.get('Wells.Databricks.KeyVault.Secrets','corvaApi')
    url = "https://api.corva.ai/v1/data/corva/operations?"
    start_timestamp = 0  
    #if is_new_well else getLatestTimeStamp(df, asset_id)
    n = 1
    while True:
        headers = {'authorization':apiKey}
        params = {
            'asset_id': asset_id,
            'limit': 10000,  # defining query limit...maximum allowed is 10k
            'query': r"{data.start_timestamp#gt#" + str(start_timestamp) + "}",
            'sort': r"{timestamp: 1}"
        }
        s = requests.Session()
        response= requests_retry_session(session=s).get(url, headers=headers,params = params)                                                     
        if len(response.json())>2:
          resp = response.json()
          start_timestamp = resp[-1]['timestamp']
          filename = str(asset_id)+"_"+str(start_timestamp)+".json"
          destfile=STAGE_OP_DIR+filename
          with open(destfile, 'w') as f:
            json.dump(resp, f)
          n += 1
          if n % 10 ==0: 
            print(f"Reach timestamp {start_timestamp} for asset {asset_id}")        
        else:
          print(f'Finish for asset={asset_id}')
          break          
for asset_id in asset_ids:
  getOperations(df_op, asset_id)

# COMMAND ----------

# MAGIC %md ## Ingest new wits data into delta table

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, to_date, regexp_replace, lpad
from pyspark.sql.types import TimestampType
import re

colsToSkip=['asset_id','company_id','provider','timestamp','RecordDateTime','InternalID']
corvamaster=spark.table("03_corva.corva_master_assets")
corvamaster1=corvamaster.selectExpr('AssetID','api_number as API','name as WellName','basin as Basin','county as County','witsml_data_frequency')

def ingestIntoDeltaTable(folder_path):
  df_raw=spark.read.format("json").load(folder_path)
  df=(df_raw.withColumn('RecordDateTime',col('timestamp').cast(TimestampType()))
        .selectExpr('asset_id','company_id','provider','timestamp','RecordDateTime','data.*').sort('timestamp')
        .withColumn('InternalID',concat(col('asset_id'),lit('~'),col('timestamp')))).dropDuplicates()  
  orig_columns=df.columns
  new_columns = [re.sub("[^a-zA-Z0-9_\n\.]","",x) for x in orig_columns]
  new_columns1=[]
  for column in new_columns:
    if column not in new_columns1:
      new_columns1.append(column)
    else:
      new_columns1.append(i+'_dup')
  final_columns=['`'+i +'`'+' as '+j for i,j in zip(orig_columns,new_columns1)]
  df=df.selectExpr(*final_columns)
  df=df.join(corvamaster1,df.asset_id==corvamaster1.AssetID,'left_outer')
  columns = ['RecordDateTime','API','WellName', 'InternalID', 'RecordAdded', 'ProcessedDate','asset_id', 
               'company_id', 'provider','timestamp','witsml_data_frequency',
               'rop', 'weight_on_bit', 'diff_press','rotary_rpm','rotary_torque', 'hole_depth', 'bit_depth', 'block_height', 'hook_load','state']
  df=((df.withColumn('RecordAdded',col('RecordDateTime')))
         .withColumn('ProcessedDate',to_date('RecordAdded'))
         .withColumn('API',regexp_replace(df.API,"[-\/]",""))
         .withColumn('API',lpad('API',10,'0')).select(*columns))
  
  return df
    
df_spark = ingestIntoDeltaTable("/AD_1s_stage/")

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, to_date, regexp_replace, lpad
from pyspark.sql.types import TimestampType
import re

colsToSkip=['asset_id','company_id','provider','timestamp','RecordDateTime','InternalID']
corvamaster=spark.table("03_corva.corva_master_assets")
corvamaster1=corvamaster.selectExpr('AssetID','api_number as API','name as WellName','basin as Basin','county as County','witsml_data_frequency')

def ingestIntoDeltaTable(folder_path):
  df_raw=spark.read.format("json").load(folder_path)
  df=(df_raw.withColumn('RecordDateTime',col('timestamp').cast(TimestampType()))
        .selectExpr('asset_id','company_id','provider','timestamp','RecordDateTime','data.*').sort('timestamp')
        .withColumn('InternalID',concat(col('asset_id'),lit('~'),col('timestamp')))).dropDuplicates()  
  orig_columns=df.columns
  new_columns = [re.sub("[^a-zA-Z0-9_\n\.]","",x) for x in orig_columns]
  new_columns1=[]
  for column in new_columns:
    if column not in new_columns1:
      new_columns1.append(column)
    else:
      new_columns1.append(i+'_dup')
  final_columns=['`'+i +'`'+' as '+j for i,j in zip(orig_columns,new_columns1)]
  df=df.selectExpr(*final_columns)
  df=df.join(corvamaster1,df.asset_id==corvamaster1.AssetID,'left_outer')
  columns = ['RecordDateTime','API','WellName', 'InternalID', 'RecordAdded', 'ProcessedDate','asset_id', 
               'company_id', 'provider','timestamp','witsml_data_frequency', 'start_bit_depth', 'end_bit_depth', 'operation_name', 'well_section']
  df=((df.withColumn('RecordAdded',col('RecordDateTime')))
         .withColumn('ProcessedDate',to_date('RecordAdded'))
         .withColumn('API',regexp_replace(df.API,"[-\/]",""))
         .withColumn('API',lpad('API',10,'0')).select(*columns))
  
  return df

df_op = ingestIntoDeltaTable("/AD_OP_stage/")

# COMMAND ----------

write_format = 'delta'
write_mode = 'append'
write_table_name = 'auto_driller_one_second'
mnt_location = f'/mnt/delta/{write_table_name}/'
write_table_location = 'sandbox'


def createDeltaTable(df):
  print('start writing to dbfs')
  df.write.format(write_format).mode(write_mode).save(mnt_location)
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

createDeltaTable(df_spark)

# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'auto_driller_op'
mnt_location = f'/mnt/delta/{write_table_name}/'
write_table_location = 'sandbox'


def createDeltaTable(df):
  print('start writing to dbfs')
  df.write.format(write_format).mode(write_mode).save(mnt_location)
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

createDeltaTable(df_op)
