# Databricks notebook source
from pyspark.sql import functions as func
from pyspark.sql.functions import col, lag, udf, stddev, min, max, first, last
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
import plotly.express as px
import seaborn as sns
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
plt.style.use('seaborn')

# COMMAND ----------

df = spark.table('sandbox.auto_driller_1s_silver').orderBy(['asset_id', 'stand_id', 'RecordDatetime'])
df_test = spark.table('sandbox.auto_driller_1s_silver_test').orderBy(['asset_id', 'stand_id', 'RecordDatetime'])

# COMMAND ----------

df = df.union(df_test)

# COMMAND ----------

display(df.select("well_name").distinct())

# COMMAND ----------

w= Window().partitionBy('asset_id', 'stand_id').orderBy('RecordDatetime')
df = df.withColumn('rop_lag1', lag("rop", offset=-1).over(w)).na.drop()
df = df.withColumn('rop_diff', col('rop')-col('rop_lag1'))
df = df.orderBy('asset_id', 'stand_id', 'RecordDateTime')
df_agg = df.groupBy('asset_id','stand_id').agg(stddev('rop_diff').alias('rop_diff'), 
                                               min('bit_depth').alias('start_bitdepth'), 
                                               max('bit_depth').alias('end_bitdepth'), 
                                               first('well_name').alias('well_name'), 
                                               ((max('RecordDateTime').cast('long')
                                                 -min('RecordDateTime')                                                
                                                 .cast('long'))/60).cast('int').alias('stand_duration'))
display(df_agg)

# COMMAND ----------

df_agg_tim = df_agg.filter(col('well_name')=='Tims 1-35H26X27X22X15')
threshold1, threshold2 = df_agg_tim.approxQuantile('rop_diff', [0.4, 0.98], relativeError=0)

# COMMAND ----------

df_agg_tim = df_agg.filter(col('well_name')=='Tims 1-35H26X27X22X15')
threshold1, threshold2 = df_agg_tim.approxQuantile('rop_diff', [0.4, 0.98], relativeError=0)
display(print(f'thred1 = {threshold1}, thred2 = {threshold2}'))
def health_categorizer(diff):
  if diff<threshold1:
    return 'good'
  elif diff<threshold2:
    return 'ok'
  else:
    return 'bad'
bucket_udf = udf(health_categorizer, StringType())
df_buck = df_agg.withColumn('health_total', bucket_udf('rop_diff'))
display(df_buck)

# COMMAND ----------

df_buck.select('well_name').distinct().show()

# COMMAND ----------

def plot_health(df):
  fig, ax = plt.subplots(figsize=(10, 5))
  colors = {'good':'green', 'bad':'red', 'ok':'orange'}
  ax.scatter(df['stand_id'], df['stand_duration'], c=df['health_total'].map(colors))
  ax.set_xlabel('stand_id')
  plt.show()

df_agg_tim = df_buck.filter(col('well_name')=='Tims 1-35H26X27X22X15').toPandas()
df_agg_45= df_buck.filter(col('well_name')=='BdC-45(h) (Aislacion)').toPandas()
df_agg_fish= df_buck.filter(col('well_name')=='Fish 1-35H26X23').toPandas()
df_agg_546= df_buck.filter(col('well_name')=='Keydets-A 47 #4H').toPandas()
df_agg_547= df_buck.filter(col('well_name')=='Panthers-Broncos 1B').toPandas()

plot_health(df_agg_tim)
plot_health(df_agg_45) 
plot_health(df_agg_fish)
plot_health(df_agg_546)
plot_health(df_agg_547)

# COMMAND ----------

df_buck.select('well_name').distinct().show()

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

threshold1, threshold2 = df_buck.approxQuantile('stand_duration', [0.01, 0.97], relativeError=0)

# COMMAND ----------

print(f'stand duration q1 = {threshold1}, q2= {threshold2}')

# COMMAND ----------

from pyspark.sql.functions import when
df_buck = df_buck.withColumn('health_total', when(df_buck.stand_duration>threshold2, "bad").otherwise(df_buck.health_total))

# COMMAND ----------

df_agg_tim = df_buck.filter(col('well_name')=='Tims 1-35H26X27X22X15').toPandas()
plot_health(df_agg_tim)

# COMMAND ----------

df_well = df_buck.filter(col('well_name').isin('Keydets-A 47 #4H', 'Panthers-Broncos 1B'))

# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'auto_driller_mvp_health_test'
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


writeToDBFS(df_well)
createDeltaTable()
