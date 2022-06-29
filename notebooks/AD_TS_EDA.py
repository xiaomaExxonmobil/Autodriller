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

df_label = spark.read.csv(r'dbfs:/FileStore/AD_labels/Pablo_50_Good_labels', header=True)

# COMMAND ----------

display(df_label)

# COMMAND ----------

df_label = df_label.toPandas()

# COMMAND ----------

sns.countplot(data=df_label, x='AD_Feedback')

# COMMAND ----------

df_label['SME_Name'].unique()

# COMMAND ----------

df_label[df_label['SME_Name']=='pablo.barajas1@exxonmobil.com'].head()

# COMMAND ----------

df_label[df_label['SME_Name']=='pablo.barajas1@exxonmobil.com'].describe()

# COMMAND ----------

df_label[(df_label['SME_Name']=='pablo.barajas1@exxonmobil.com') & (df_label['AD_Feedback']=='good')].head()

# COMMAND ----------

df_label_good = df_label[(df_label['SME_Name']=='pablo.barajas1@exxonmobil.com') & (df_label['AD_Feedback']=='good')]

# COMMAND ----------

sns.countplot(data=df_label_good, x='AD_Feedback')

# COMMAND ----------

df_health = df_health.toPandas()

# COMMAND ----------

df_health.head()

# COMMAND ----------

df_bad= df_health[df_health['health_total']=='bad'][['asset_id', 'stand_id',  'stand_duration', 'health_total']].sample(50, random_state=42)
#f_bad = df_label[df_label['health_total']=='bad'][['asset_id', 'stand_id',  'stand_duration', 'health_total']].sample(50, random_state=42)

# COMMAND ----------

df_label_good['health_total'] = df_label_good['AD_Feedback']

# COMMAND ----------

df_good = df_label_good[['asset_id', 'stand_id',  'stand_duration', 'health_total']]

# COMMAND ----------

import numpy as np

# COMMAND ----------

convert_dict = {'asset_id': np.int64,
                'stand_id': np.int32,
                'stand_duration': np.int32,
                'health_total': str}
df_good = df_good.astype(convert_dict)

# COMMAND ----------

df_good.dtypes

# COMMAND ----------

df_bad.dtypes

# COMMAND ----------

df_label_new = pd.concat([df_good, df_bad])

# COMMAND ----------

sns.countplot(data=df_label_new, x='health_total')

# COMMAND ----------

df_label_spark = spark.createDataFrame(df_label_new)

# COMMAND ----------

df_data = df_1s.join(df_label_spark, how='inner', on=['asset_id', 'stand_id'])

# COMMAND ----------

columns = ['asset_id', 'stand_id', 'well_name', 'RecordDateTime', 'bit_depth', 'rop', 'weight_on_bit','stand_duration', 'health_total']
df = df_data.select(columns).toPandas()

# COMMAND ----------

# MAGIC %md **Get the 1 second data with labels**

# COMMAND ----------

# MAGIC %md check Null values

# COMMAND ----------

df.isna().sum()

# COMMAND ----------

df_train = df[['asset_id', 'well_name', 'stand_id', 'RecordDateTime', 'rop', 'weight_on_bit', 'health_total']]

# COMMAND ----------

df_train.head()

# COMMAND ----------

df_train['new_id'] = df_train['asset_id'].astype('str') + '~' + df_train['stand_id'].astype('str')

# COMMAND ----------

# MAGIC %md ## Feature Engineering

# COMMAND ----------

df_temp = df_train[['new_id', 'RecordDateTime', 'rop', 'weight_on_bit', 'health_total']]

# COMMAND ----------

df_temp.head()

# COMMAND ----------

df_temp.dropna()

# COMMAND ----------

from tsfresh import extract_features
from tsfresh.feature_extraction import EfficientFCParameters, MinimalFCParameters

# extracted_features = extract_features(df_temp[['stand_id', 'RecordDateTime', 'rop']], column_id="stand_id", 
#                                       column_sort="RecordDateTime")

extracted_features = extract_features(df_temp[['new_id', 'RecordDateTime', 'rop', 'weight_on_bit']], column_id='new_id', 
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

df_train_new = extracted_features
df_train_new['Label'] = y

# COMMAND ----------

df_train_new.head()

# COMMAND ----------

#num_cols = ['rop__sum_values', 'rop__median', 'rop__mean', 'rop__length']
num_cols = [col for col in df_train_new.columns if col!= 'Label']
plt.figure(figsize=(25,9))
for i, col_i in enumerate(num_cols):
  plt.subplot(2,len(num_cols)//2,i+1) 
  g = sns.boxenplot(x="Label", y=col_i, data=df_train_new, showfliers=False)
plt.tight_layout(pad=0.4, w_pad=0.5, h_pad=1.0)

# COMMAND ----------

sns.countplot(data=df_train_new, x='Label')

# COMMAND ----------

# extracted_features2 = extract_features(df_temp[['new_id', 'RecordDateTime', 'rop', 'weight_on_bit']], column_id='new_id', 
#                                       column_sort="RecordDateTime",
#                                     default_fc_parameters= EfficientFCParameters())

# COMMAND ----------

sns.countplot(data=df_temp, x='health_total')

# COMMAND ----------

extracted_features.shape

# COMMAND ----------

from tsfresh import select_features
from tsfresh.utilities.dataframe_functions import impute
import tsfresh

impute(extracted_features)
features_filtered = select_features(extracted_features, y)

# COMMAND ----------

kind_to_fc_parameters = tsfresh.feature_extraction.settings.from_columns(extracted_features)
print(kind_to_fc_parameters)

# COMMAND ----------

X_new = extracted_features

# COMMAND ----------

y = df_temp.groupby(['new_id'])['health_total'].apply(lambda x: x.min()).map({'good':0, 'bad':1})

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X_new, y, test_size=0.3, random_state=42, stratify=y)

# COMMAND ----------

# CV to find the best hyper-parameters
param_grid = {'penalty' : ['l1','l2'],
              'C' : [0.1, 0.2, 0.5, 1, 1.5, 2] ,
              'solver': ['saga', 'liblinear']}

grid_clf = GridSearchCV(LogisticRegression(max_iter = 300), param_grid, cv=5,verbose=10, scoring = 'f1')
grid_clf.fit(X_train, y_train.ravel())
grid_clf.best_estimator_ # show the best model

# COMMAND ----------

lg_model = LogisticRegression(penalty='l1', C = 1.5, fit_intercept=True, solver='saga',max_iter=300)#,class_weight ='balanced')
lg_model.fit(X_train, y_train.ravel())

# COMMAND ----------

from sklearn.metrics import confusion_matrix,roc_curve, auc, recall_score, accuracy_score, precision_score,plot_precision_recall_curve,precision_recall_curve,plot_roc_curve

def plot_roc_and_precision_recall(model, X_train, y_train, X_test, y_test, model_name):
    """
    This function plots the roc curve and precision-recall curve for both training and test data
    
    Inputs:
        model: a fitted classification model from sklearn
        X_train: feature data in the training set
        y_train: predicted data in the training set
        X_test:  feature data in the test set
        y_test:  predicted data in the test set
        model_name: a str-type name given to the model
    Outputs:
        auc_train: AUC for the training dataset
        auc_test: AUC for the test dataset
        Two plots for roc and precision-recall
    """

    #plot the roc curve for training and test data
    fig = plot_roc_curve( model, X_train, y_train, label = '%s, Train'%model_name)
    fig = plot_roc_curve( model, X_test, y_test, ax = fig.ax_, label = '%s, Test'%model_name)
    fig.figure_.suptitle("ROC curve comparison")
    plt.plot([0, 1], [0, 1], 'k--')
    plt.show()
    # make predictions for training and test data
    pred_train = model.predict(X_train)
    pred_test = model.predict(X_test)
    # calculate auc for the prediction of training and test data
    fpr, tpr, thresholds_roc = roc_curve(y_train, pred_train)
    auc_train = auc(fpr, tpr)
    fpr, tpr, thresholds_roc = roc_curve(y_test, pred_test)
    auc_test = auc(fpr, tpr)
    print('AUC (area under roc curve) for training data is: %f'%auc_train)
    print('AUC (area under roc curve) for test data is: %f \n'%auc_test)
    # plot precision-recall
    fig = plot_precision_recall_curve(model, X_train, y_train, label = '%s, Train'%model_name)
    fig = plot_precision_recall_curve(model, X_test, y_test,ax = fig.ax_, label = '%s, Test'%model_name)
    fig.figure_.suptitle("Precision-recall curve comparison")
    plt.show()

    return auc_train, auc_test
  
def eval_metrics(y_train, pred_train, y_test, pred_test):
    """
    This function returns a matrix of recall, precision, and accuracy for both training and test data.
    Input types are the same as those for predictions_optimal_prob (excluding model type and p_limit)
    Outputs:
        outs: a dataframe listing the key metrics
    """

    outs = pd.DataFrame()
    outs['Metrics'] = ['Recall_Training', 'Recall_Test','Precision_Training','Precision_Test','Accuracy_Training', 'Accuracy_Test']
    outs['Value'] = [recall_score(y_train, pred_train), recall_score(y_test,pred_test),  
                    precision_score( y_train, pred_train), precision_score(y_test,pred_test),
                    accuracy_score(y_train, pred_train),  accuracy_score(y_test,pred_test) ]

    outs['Value'] = round(outs['Value'], 4)
    return outs
  
  
def predictions_optimal_prob(model, X_train, X_test, y_train, y_test, p_limit):
  
    """
    This function returns the predictions for training and test data following the logic described above.
    
    Inputs:
        model: a fitted classification model from sklearn
        X_train: feature data in the training set
        y_train: predicted data in the training set
        X_test:  feature data in the test set
        y_test:  predicted data in the test set
        p_limit: lower-bound limit for precision
    Outputs:
        pred_train: predictions for the training dataset
        pred_test: predictions for the test dataset
        df_prec_recall: a dataframe summarizing the precision and recall
    """
    # predictions in probability 
    pred_train_proba = model.predict_proba(X_train)[:,1]
    pred_test_proba = model.predict_proba(X_test)[:,1]
    # precision and recall scatters
    precision_train, recall_train, thresholds_train = precision_recall_curve(y_train, pred_train_proba)
    precision_test, recall_test, thresholds_test = precision_recall_curve(y_test, pred_test_proba)

    df_prec_recall = pd.DataFrame()
    df_prec_recall['thresholds'] = thresholds_test
    df_prec_recall['recall'] = recall_test[1:]
    df_prec_recall['precision'] = precision_test[1:]
    # make final predictions based on thresholds_test defined above
    prob_thresh = df_prec_recall[df_prec_recall['precision'] > p_limit].sort_values(by = 'recall', ascending=False).reset_index().drop(['index'], axis=1)['thresholds'][0]
    pred_train = 1*(pred_train_proba > prob_thresh)
    pred_test = 1*(pred_test_proba > prob_thresh)

    return pred_train, pred_test, df_prec_recall


# COMMAND ----------

lg_auc_train, lg_auc_test = plot_roc_and_precision_recall(lg_model, X_train, y_train, X_test, y_test, model_name = 'Logistic')

# COMMAND ----------

X_train.head()

# COMMAND ----------

lg_auc_train

# COMMAND ----------

lg_auc_test

# COMMAND ----------

lg_pred_train, lg_pred_test, lg_df_prec_recall = predictions_optimal_prob(lg_model, X_train, X_test, y_train, y_test, p_limit = 0.6)

# COMMAND ----------

lg_outs = eval_metrics(y_train, lg_pred_train, y_test, lg_pred_test)
lg_outs

# COMMAND ----------

X_train

# COMMAND ----------

!pip install mlflow

# COMMAND ----------

!pip install hyperopt

# COMMAND ----------

# X_val = X_test
# y_val = y_test

# import mlflow.xgboost
# import numpy as np
# import xgboost as xgb
# from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
# from hyperopt.pyll import scope
# from mlflow.models.signature import infer_signature
# from mlflow.utils.environment import _mlflow_conda_env
# from sklearn.metrics import roc_auc_score

 
# search_space = {
#   'max_depth': scope.int(hp.quniform('max_depth', 4, 100, 1)),
#   'learning_rate': hp.loguniform('learning_rate', -3, 0),
#   'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
#   'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
#   'min_child_weight': hp.loguniform('min_child_weight', -1, 3),
#   'objective': 'binary:logistic',
#   'seed': 123, # Set a seed for deterministic training
# }
 
# def train_model(params):
#   # With MLflow autologging, hyperparameters and the trained model are automatically logged to MLflow.
#   mlflow.xgboost.autolog()
#   with mlflow.start_run(nested=True):
#     train = xgb.DMatrix(data=X_train, label=y_train)
#     validation = xgb.DMatrix(data=X_val, label=y_val)
#     # Pass in the validation set so xgb can track an evaluation metric. XGBoost terminates training when the evaluation metric
#     # is no longer improving.
#     booster = xgb.train(params=params, dtrain=train, num_boost_round=1000,\
#                         evals=[(validation, "validation")], early_stopping_rounds=50)
#     validation_predictions = booster.predict(validation)
#     auc_score = roc_auc_score(y_val, validation_predictions)
#     mlflow.log_metric('auc', auc_score)
 
#     signature = infer_signature(X_train, booster.predict(train))
#     mlflow.xgboost.log_model(booster, "model", signature=signature)
    
#     # Set the loss to -1*auc_score so fmin maximizes the auc_score
#     return {'status': STATUS_OK, 'loss': -1*auc_score, 'booster': booster.attributes()}
 
# # Greater parallelism will lead to speedups, but a less optimal hyperparameter sweep. 
# # A reasonable value for parallelism is the square root of max_evals.
# spark_trials = SparkTrials(parallelism=10)
 
# # Run fmin within an MLflow run context so that each hyperparameter configuration is logged as a child run of a parent
# # run called "xgboost_models" .
# with mlflow.start_run(run_name='xgboost_models'):
#   best_params = fmin(
#     fn=train_model, 
#     space=search_space, 
#     algo=tpe.suggest, 
#     max_evals=96,
#     trials=spark_trials,
#   )
