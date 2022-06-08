# Databricks notebook source
from pyspark.sql import functions as func
from pyspark.sql.functions import col, lag, udf, stddev, min, max, first, last
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from plotly.subplots import make_subplots
import plotly.express as px
import seaborn as sns
import warnings
import pickle
warnings.filterwarnings("ignore", category=FutureWarning)
plt.style.use('seaborn')

# COMMAND ----------

df = spark.table('03_corva.corva_drilling_wits_assetid')

# COMMAND ----------

df = df.select("asset_id", 'WellName', 'timestamp', 'RecordDateTime','rop')

# COMMAND ----------

display(df)

# COMMAND ----------

well_name = 'Tims 1-35H26X27X22X15'
df_well = df.filter(col('WellName')==well_name)

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
import json
import os

def getOperations(asset_id):
  apiKey = dbutils.secrets.get('Wells.Databricks.KeyVault.Secrets','corvaApi')
  #apiKey = dbutils.secrets.get('Wells.Databricks.KeyVault.Secrets','corvaAPI-readwrite')

  url = 'https://data.corva.ai/api/v1/data/corva/wits/?'
  start_timestamp = 0  # initializing a starting value
  output = []
  n=5
  while n:
    headers = {'authorization':apiKey}
    query = json.dumps({'asset_id': asset_id,
                       # 'timestamp#eq#':start_timestamp,
                        })
    sort = json.dumps({'data.time': -1})
    r = requests.get(url, headers=headers,
                     params = {"sort":json.dumps({'timestamp':1}),
                               'limit': 10000,
                               'query': query,
                               'timestamp#gte#': start_timestamp,
                               #'data.time#gte#': '2021-10-20T07:00:00-06:00',
                               #'data.time#lte#': '2021-10-20T08:30:00-06:00',
                               })
    resp = r.json()
    if len(resp) > 0:
      start_timestamp = resp[-1]['timestamp']
      output.extend(resp)  # concatenate to other responses
    else:
        break
    n -=1
    print(n)
  return pd.json_normalize(output)

df_wits_well = getOperations(asset_id = 27509614)

# COMMAND ----------

df_wits_well['data.time'].max()

# COMMAND ----------

df_wits_well['data.time'].min()

# COMMAND ----------

df_wits_well['data.']

# COMMAND ----------

plt.plot(df_wits_well['data.time'], df_wits_well['data.rop'])
plt.show()

# COMMAND ----------

plt.plot(pd.to_datetime(df_wits_well['data.time']), df_wits_well['data.rop'])

# COMMAND ----------

#dbutils.fs.mkdirs('Pickles')

# COMMAND ----------

#dbutils.fs.put("/Pickles/df.pkl", "This is a file in cloud storage.")

# with open('/dbfs/Pickles/df.pkl', 'wb') as f:
#   pickle.dump(df, f)

# COMMAND ----------

df = pd.DataFrame()

# COMMAND ----------

with open('/dbfs/Pickles/df.pkl', 'rb') as f:
  df = pickle.load(f)

# COMMAND ----------

print('data shape:', df.shape)

# COMMAND ----------

df.head()

# COMMAND ----------

# MAGIC %md # 1. EDA

# COMMAND ----------

# MAGIC %md **Number of stants**

# COMMAND ----------

df.groupby(['well_name'])['stand_id'].nunique()

# COMMAND ----------

# MAGIC %md **Check stand level stats**

# COMMAND ----------

df_well = df[(df['well_name']=='Tims 1-35H26X27X22X15')]

# COMMAND ----------

# MAGIC %md **plot the ts without state**

# COMMAND ----------

def plot_stand(df, columns, stand_id):
  mask_stand = (df['stand_id']==stand_id)
  fig = plt.figure(figsize=(10,5))
  i_figure=1
  for column in columns:
    plt.subplot(len(columns),1, i_figure)
    plt.plot(df[mask_stand]["RecordDateTime"], df[mask_stand][column], marker='o', markersize=5)
    plt.xlabel('RecordDateTime')
    plt.ylabel(column)
    i_figure +=1
  plt.tight_layout()
  plt.show()
  
plot_stand(df_well, columns = ['rop','weight_on_bit'], stand_id = 141)

# COMMAND ----------

plot_stand(df_well[(df_well['state']=='Rotary Drilling')],
           columns = ['rop','weight_on_bit'], stand_id = 141)

# COMMAND ----------

# MAGIC %md Truncate the time series by removing the artifacts from the start and the end of the stand

# COMMAND ----------

#df_well = df[(df['well_name']=='Tims 1-35H26X27X22X15')]
#df_well = df_well[~(df_well['state']=='In Slips')]
min_time = df.groupby(['asset_id', 'stand_id'])['RecordDateTime'].transform('min') + pd.Timedelta(5, unit = 'm')
max_time = df.groupby(['asset_id', 'stand_id'])['RecordDateTime'].transform('max') - pd.Timedelta(5, unit = 'm')
mask_time = ((df['RecordDateTime']>min_time) & (df['RecordDateTime'] < max_time))
df_trunc = df[mask_time]

# COMMAND ----------

df_well = 

# COMMAND ----------

plot_stand(df_trunc[(df_trunc['well_name']=='Tims 1-35H26X27X22X15')],
           columns = ['rop','weight_on_bit'], stand_id = 128)
plot_stand(df[(df['well_name']=='Tims 1-35H26X27X22X15')],
           columns = ['rop','weight_on_bit'], stand_id = 128)

# COMMAND ----------

# MAGIC %md start and end artifacts are removed from the TS 

# COMMAND ----------

# MAGIC %md **Look into the stand duration**

# COMMAND ----------

df_agg = df.groupby(['asset_id', 'well_name', 'stand_id']).agg({'RecordDateTime': ['min', 'max']})
df_agg.columns = ['start_time', 'end_time']
df_agg = df_agg.reset_index()
df_agg['stand_duration'] = (df_agg['end_time']
                            -df_agg['start_time']).dt.seconds.div(60).astype(int)

# COMMAND ----------

df_agg['stand_duration'].describe()

# COMMAND ----------

plt.hist(x=df_agg['stand_duration'], bins = 30)
plt.xlabel('stand_duration')
plt.title('Histogram for stand durations')

# COMMAND ----------

plt.hist(x=df_agg['stand_duration'], bins = 30, range=df_agg['stand_duration'].quantile([0.01, 0.95]))
plt.xlabel('stand_duration')
plt.title('Histogram for stand durations (No Outliers)')

# COMMAND ----------

q1, q2 = df_agg['stand_duration'].quantile([0.01, 0.95]).tolist()
print(f'q1={q1}, q2={q2}')

# COMMAND ----------

mask_quantile = ((df_agg['stand_duration']>q1) & (df_agg['stand_duration']<q2))
df_agg[~mask_quantile].sort_values('stand_duration')

# COMMAND ----------

df.withColumn()

# COMMAND ----------

df.select('asset_id', 'well_name')

# COMMAND ----------

df_count = df.groupBy('asset_id', 'well_name', 'stand_id').count().toPandas()

# COMMAND ----------

def calculateLagSTD(df, columns=['rop']):
  w= Window().partitionBy('asset_id', 'stand_id').orderBy('RecordDatetime')
  for column in columns:
    column_lag = column + '_lag1'
    column_diff = column + '_diff'
    df = df.withColumn(column_lag, lag(column, offset=-1).over(w)).na.drop()
    df = df.withColumn(column_diff, col(column)-col(column_lag))
  df = df.orderBy('asset_id', 'stand_id', 'RecordDateTime')                                               
  df_agg = df.groupBy('asset_id','stand_id').agg(stddev(col('rop_diff')).alias('rop_diff_std'),
                                                 stddev(col('weight_on_bit_diff'))
                                                 .alias('weight_on_bit_diff_std'),       
                                                 min('bit_depth').alias('start_bitdepth'), 
                                                 max('bit_depth').alias('end_bitdepth'), 
                                                 first('well_name').alias('well_name'),
                                                 ((max('RecordDateTime').cast('long')
                                                   -min('RecordDateTime').cast('long'))/60)
                                                 .alias('stand_duration'))
  return df_agg
  
df_agg = calculateLagSTD(df, columns = ['rop', 'weight_on_bit'])

# COMMAND ----------

display(df_agg)

# COMMAND ----------

df_pd = df_agg.toPandas()

# COMMAND ----------

import plotly.express as px
fig = px.scatter(df_pd, x='rop_diff_std', y='weight_on_bit_diff_std', 
                 color = 'well_name',
                 hover_data=['well_name', 'stand_id'])
fig.show()

# COMMAND ----------

sns.scatterplot(data=df_pd, x='weight_on_bit_diff_std', y='rop_diff_std')

# COMMAND ----------

df_agg_tim = df_agg.filter(col('well_name')=='Tims 1-35H26X27X22X15')
threshold1, threshold2 = df_agg_tim.approxQuantile('rop_diff_std', [0.4, 0.98], relativeError=0)

# COMMAND ----------

df_agg_tim = df_agg.filter(col('well_name')=='Tims 1-35H26X27X22X15')
threshold1, threshold2 = df_agg_tim.approxQuantile('rop_diff_std', [0.4, 0.98], relativeError=0)
display(print(f'thred1 = {threshold1}, thred2 = {threshold2}'))
def health_categorizer(diff):
  if diff<threshold1:
    return 'good'
  elif diff<threshold2:
    return 'ok'
  else:
    return 'bad'
bucket_udf = udf(health_categorizer, StringType())
df_buck = df_agg.withColumn('health_total', bucket_udf('rop_diff_std'))
display(df_buck)

# COMMAND ----------

def plot_health(df):
  fig, ax = plt.subplots(figsize=(10, 5))
  colors = {'good':'green', 'bad':'red', 'ok':'orange'}
  ax.scatter(df['stand_id'], df['rop_diff_std'], c=df['health_total'].map(colors))
  ax.set_xlabel('stand_id')
  plt.show()

df_agg_tim = df_buck.filter(col('well_name')=='Tims 1-35H26X27X22X15').toPandas()
df_agg_45= df_buck.filter(col('well_name')=='BdC-45(h) (Aislacion)').toPandas()
df_agg_fish= df_buck.filter(col('well_name')=='Fish 1-35H26X23').toPandas()

plot_health(df_agg_tim)
plot_health(df_agg_45)
plot_health(df_agg_fish)

# COMMAND ----------

#Add Constant columns to avoid information relink in dashboard
from pyspark.sql.functions import lit
df_buck = df_buck.drop('rop_diff')
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
  df_buck = df_buck.withColumn(column, lit(999))

# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'auto_driller_mvp_health'
mnt_location = f'/mnt/delta/{write_table_name}/'
write_table_location = 'sandbox'
def writeToDBFS(df):
    print('start writing to dbfs')
    df.write.format(write_format).\
      mode(write_mode).\
      option("overwriteSchema", "true").\
      save(mnt_location)
    print('finish writing to dbfs')
        
def createDeltaTable():
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

writeToDBFS(df_buck)
createDeltaTable()
