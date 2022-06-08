# Databricks notebook source
!pip install -U tsfresh
!pip install -U seaborn
!pip install -U mlflow
!pip install -U hyperopt

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
#import mlflow
warnings.filterwarnings("ignore", category=FutureWarning)
plt.style.use('seaborn')

# COMMAND ----------

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

write_format = 'delta'
write_mode = 'overwrite'
write_table_name = 'auto_driller_train'
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

# COMMAND ----------

df_1s = loadFromSQLDB('auto_driller_1s')
df_health = loadFromSQLDB('auto_driller_mvp_fools_gold')

# COMMAND ----------

df_label = df_health.toPandas()

# COMMAND ----------

sns.countplot(data=df_label, x='health_total')

# COMMAND ----------

mask_good = (df_label['health_total']=='bad')
df_label[mask_good][['asset_id', 'stand_id']].count()

# COMMAND ----------

mask_good = (df_label['health_total']!='bad')
df_label[mask_good][['asset_id', 'stand_id']].count()

# COMMAND ----------

import numpy as np

# COMMAND ----------

df_label['health_total'] = df_label['health_total'].map({'ok':'good', 'good':'good', 'bad':'bad'})

# COMMAND ----------

df_label.head()

# COMMAND ----------

df_label[['asset_id', 'stand_id', 'health_total']]

# COMMAND ----------

sns.countplot(data=df_label, x='health_total')

# COMMAND ----------

df_bad = df_label[df_label['health_total']=='bad'][['asset_id', 'stand_id',  'stand_duration', 'health_total']].sample(50, random_state=42)

# COMMAND ----------

df_good = df_label[df_label['health_total']=='good'][['asset_id', 'stand_id',  'stand_duration', 'health_total']].sample(50, random_state=42)

# COMMAND ----------

df_label_new = pd.concat([df_good, df_bad])

# COMMAND ----------

plt.hist(x=df_label['stand_duration'], bins = 30, range=df_label['stand_duration'].quantile([0.1, 0.9]))

# COMMAND ----------

df_label_spark = spark.createDataFrame(df_label_new)

# COMMAND ----------

df_data = df_1s.join(df_label_spark, how='inner', on=['asset_id', 'stand_id'])

# COMMAND ----------

display(df_data)

# COMMAND ----------

columns = ['asset_id', 'stand_id', 'well_name', 'RecordDateTime', 'bit_depth', 'rop', 'weight_on_bit', 'stand_duration', 'health_total']
df = df_data.select(columns).toPandas()

# COMMAND ----------

# MAGIC %md **Get the 1 second data with labels**

# COMMAND ----------

# MAGIC %md check Null values

# COMMAND ----------

df.isna().sum()

# COMMAND ----------

df_train = df[['asset_id', 'well_name', 'stand_id', 'RecordDateTime', 'rop', 'health_total']]

# COMMAND ----------

df_train.head()

# COMMAND ----------

# MAGIC %md ## Feature Engineering

# COMMAND ----------

df_temp = df_train[['asset_id', 'stand_id', 'RecordDateTime', 'rop', 'health_total']]

# COMMAND ----------

df_temp.head()

# COMMAND ----------

df_temp['new_id'] = df_temp['asset_id'].astype('str') + '~' + df_temp['stand_id'].astype('str')

# COMMAND ----------

df_temp = df_temp[['new_id', 'asset_id', 'stand_id', 'RecordDateTime', 'rop', 'health_total']]

# COMMAND ----------

df_temp.head()

# COMMAND ----------

from tsfresh import extract_features
from tsfresh.feature_extraction import EfficientFCParameters, MinimalFCParameters

# extracted_features = extract_features(df_temp[['stand_id', 'RecordDateTime', 'rop']], column_id="stand_id", 
#                                       column_sort="RecordDateTime")

extracted_features = extract_features(df_temp[['new_id', 'RecordDateTime', 'rop']], column_id='new_id', 
                                      column_sort="RecordDateTime",
                                    default_fc_parameters= MinimalFCParameters())

# COMMAND ----------

extracted_features.head()

# COMMAND ----------

extracted_features.shape

# COMMAND ----------

y = df_temp.groupby(['new_id'])['health_total'].apply(lambda x: x.min()).map({'good':0, 'bad':1})

# COMMAND ----------

y.shape

# COMMAND ----------

from tsfresh import select_features
from tsfresh.utilities.dataframe_functions import impute
import tsfresh

impute(extracted_features)
features_filtered = select_features(extracted_features, y)

# COMMAND ----------

features_filtered.head()

# COMMAND ----------

re_table = tsfresh.feature_selection.relevance.calculate_relevance_table(extracted_features, y)

# COMMAND ----------

re_table.sort_values('p_value',ascending=False )

# COMMAND ----------

from sklearn.feature_selection import chi2
from sklearn.feature_selection import SelectKBest
from sklearn.feature_selection import SelectFromModel
from sklearn.ensemble import ExtraTreesClassifier
# X_new = SelectKBest(chi2, k=10).fit_transform(extracted_features, y)
clf = ExtraTreesClassifier(n_estimators=50)
clf = clf.fit(extracted_features, y)
clf.feature_importances_  
model = SelectFromModel(clf, prefit=True)
X_new = model.transform(extracted_features)

# COMMAND ----------

extracted_features.shape

# COMMAND ----------

kind_to_fc_parameters = tsfresh.feature_extraction.settings.from_columns(extracted_features)
print(kind_to_fc_parameters)

# COMMAND ----------

X_new.shape

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X_new, y, test_size=0.3, random_state=42)


# COMMAND ----------

!pip install mlflow

# COMMAND ----------

import mlflow

# COMMAND ----------

mlflow.autolog()

# COMMAND ----------

import sklearn
with mlflow.start_run(run_name='random_forest') as run:
  model_2 = RandomForestClassifier(
    random_state=0, 
    max_depth=20
  )
  model_2.fit(X_train, y_train)

  predicted_probs = model_2.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

import sklearn
with mlflow.start_run(run_name='gradient_boost') as run:
  model = sklearn.ensemble.GradientBoostingClassifier(random_state=0)

  model.fit(X_train, y_train)

  predicted_probs = model.predict_proba(X_test)
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs[:,1])
  mlflow.log_metric("test_auc", roc_auc)
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

model.predict_proba(X_test)

# COMMAND ----------

import seaborn as sns
from sklearn.metrics import confusion_matrix

sns.heatmap(confusion_matrix(model.predict(X_test), y_test), annot=True)

# COMMAND ----------


