# Databricks notebook source
# MAGIC %run ./Constants

# COMMAND ----------

# MAGIC %md # Import Libraries

# COMMAND ----------

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
from datetime import date
warnings.filterwarnings("ignore", category=FutureWarning)
plt.style.use('seaborn')

# COMMAND ----------

# HTTP connection
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
  
#Get 
def getLatestTimeStamp(df, asset_id):
  return df_1s.filter(col('asset_id')==asset_id).select(max('timestamp')).collect()[0][0]  

# COMMAND ----------

#Function to return JSON data 
def writeRawData(asset_id,url,start_timestamp,destination_directory):
  n = 1
  while True:
    try:
        headers = {'authorization':apiKey}
        query = json.dumps({'asset_id': asset_id, 'timestamp':{"$gte":start_timestamp}})

        params = {
            'limit': LIMIT,  # defining query limit...maximum allowed is 10k
            'query': query,
            'sort': json.dumps({'timestamp':1})
        }
        s = requests.Session()
        response= requests_retry_session(session=s).get(url,headers=headers,params = params) 

        if len(response.json())>2:
          resp = response.json()
          start_timestamp = resp[-1]['timestamp']
          filename = str(start_timestamp)+".json"
          destfile= destination_directory+filename
          with open(destfile, 'w') as f:
            json.dump(resp, f)
            print(f"writing to file {destfile}")
          n += 1
          if n % 10 ==0: 
            print(f"Reach timestamp {start_timestamp} for asset {asset_id}")        
        else:
          print(f'Finish for asset={asset_id}')
          break 
    except Exception as e:
        print(e)
