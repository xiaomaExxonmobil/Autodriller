# Databricks notebook source
# MAGIC %md 
# MAGIC ## Corva API Configurations

# COMMAND ----------

apiKey = dbutils.secrets.get('Wells.Databricks.KeyVault.Secrets','corvaApi')
operations_url = "https://api.corva.ai/v1/data/corva/operations?"
wits_url = "https://data.corva.ai/api/v1/data/corva/wits/?"

corva_api = {"wits":wits_url,
             "operations":operations_url}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storage Configuration

# COMMAND ----------

PREFIX = '/dbfs'
raw_file_path = "/dbfs/mnt/auto_driller/dev/{data_type}_stage/{asset}/"
AD_1S_STAGE =  "/mnt/auto_driller/dev/AD_1s_stage/"
# AD_OP_STAGE = "/mnt/auto_driller/dev/AD_OP_stage/"
# STAGE_DIR = PREFIX + AD_1S_STAGE
# STAGE_OP_DIR = PREFIX + AD_OP_STAGE
LIMIT = 10000 #Corva API limit
BRONZE_STORAGE = "/mnt/delta/auto_driller/bronze/"
SILVER_STORAGE = "/mnt/delta/auto_driller/silver/"
