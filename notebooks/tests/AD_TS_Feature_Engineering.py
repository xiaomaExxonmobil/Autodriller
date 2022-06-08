# Databricks notebook source
!pip install -U tsfresh
!pip install -U seaborn

# COMMAND ----------

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
from pyspark.sql.functions import col
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as f
from pyspark.sql import Window
import numpy as np
warnings.filterwarnings("ignore", category=FutureWarning)
plt.style.use('seaborn')

# COMMAND ----------

df_1s = spark.table("sandbox.auto_driller_1s_silver_test")

# COMMAND ----------

df_train = df_1s.select('asset_id', 'well_name', 'stand_id', 'RecordDateTime', 'rop').toPandas()

# COMMAND ----------

# MAGIC %md ## Feature Engineering

# COMMAND ----------

df_temp = df_train[['asset_id', 'stand_id', 'RecordDateTime', 'rop']]

# COMMAND ----------

df_temp.head()

# COMMAND ----------

df_temp['new_id'] = df_temp['asset_id'].astype('str') + '~' + df_temp['stand_id'].astype('str')

# COMMAND ----------

df_temp = df_temp[['new_id', 'asset_id', 'stand_id', 'RecordDateTime', 'rop']]

# COMMAND ----------

df_temp.head()

# COMMAND ----------

# from tsfresh import extract_features
# from tsfresh.feature_extraction import EfficientFCParameters, MinimalFCParameters

# # extracted_features = extract_features(df_temp[['stand_id', 'RecordDateTime', 'rop']], column_id="stand_id", 
# #                                       column_sort="RecordDateTime")

# extracted_features = extract_features(df_temp[['new_id', 'RecordDateTime', 'rop']], column_id='new_id', 
#                                       column_sort="RecordDateTime",
#                                     default_fc_parameters= MinimalFCParameters())

# COMMAND ----------

# kind_to_fc_parameters = tsfresh.feature_extraction.settings.from_columns(extracted_features)
# print(kind_to_fc_parameters)
kind_to_fc_parameters = {'rop': {'sum_values': None, 'median': None, 'mean': None, 'length': None, 'standard_deviation': None, 'variance': None, 'root_mean_square': None, 'maximum': None, 'absolute_maximum': None, 'minimum': None}}

# COMMAND ----------

from tsfresh import extract_features
from tsfresh.feature_extraction import EfficientFCParameters, MinimalFCParameters

extracted_features = extract_features(df_temp[['new_id', 'RecordDateTime', 'rop']], column_id='new_id', 
                                      column_sort="RecordDateTime",
                                    kind_to_fc_parameters= kind_to_fc_parameters)

# COMMAND ----------

extracted_features.head()

# COMMAND ----------

extracted_features.reset

# COMMAND ----------

df_spark = spark.createDataFrame(extracted_features.reset_index(drop=False))

# COMMAND ----------

display(df_spark)

# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'auto_driller_ml_feature_table'
mnt_location = f'/mnt/delta/{write_table_name}/'
write_table_location = 'sandbox'


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

createDeltaTable(df_spark)

# COMMAND ----------


