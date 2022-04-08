# Databricks notebook source
import pyspark.sql
from pyspark.sql import functions as func
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
import plotly.express as px
import seaborn as sns

config = pyspark.SparkConf().setAll([('spark.executor.memory', '8g'), ('spark.executor.cores', '4'), ('spark.cores.max', '4'), ('spark.driver.memory','8g')])
sc = pyspark.SparkContext.getOrCreate(conf=config)
spark = pyspark.sql.session.SparkSession(sc)

class StandIngestion(object):    
    def __init__(self, well_names, save=False):
        self.save = save
        self.well_names = well_names
        self.asset_ids = []
        self.corva_ops_table = '03_corva.corva_operations_connections_silver'
        self.corva_wits_table = '03_corva.corva_drilling_wits_silver'
        self.df_wits = None
        self.df_ops = None
        self.df_master= None
        self.min_stand_length = 20.0
        self.max_stand_length = 100.0
        self.write_format = 'delta'
        self.write_mode = 'overwrite'
        self.write_table_name = 'auto_driller_1s_silver'
        self.mnt_location = f'/mnt/delta/{self.write_table_name}/'
        self.write_table_location = 'sandbox'
        
    def getMasterTable(self):
      self.df_master = (spark.table("03_corva.corva_master_assets"))
      self.df_master = self.df_master.select("AssetID", "name", "latitude", "longitude")
      self.df_master = self.df_master.withColumnRenamed('name', 'well_name')
      self.df_master = self.df_master.withColumnRenamed('AssetID', 'asset_id')
      return self.df_master
    
    def getAllAssetIDs(self):
      df_master = self.getMasterTable()
      asset_ids = self.df_master.filter(col('well_name')
                                        .isin(self.well_names)).select('asset_id').distinct().collect()
      self.asset_ids = [int(asset_id[0]) for asset_id in asset_ids]
      return self.asset_ids
    
    def getWindow(self):
        window = Window.partitionBy().orderBy('asset_id')
        return window

    def getOpsTable(self):
      df_op = (spark.table("03_corva.corva_operations_connections_silver")
      .filter(col('operation_name')=="Drilling(Connection)")
      .orderBy('assetid',col('recorddatetime').asc()))
      df_op = df_op.select("assetid", "api_number", "name", "RecordDateTime", "start_bit_depth" )
      df_op = df_op.withColumnRenamed('name', 'well_name')
      df_op = df_op.withColumnRenamed('RecordDateTime', 'start_time')
      df_op = df_op.withColumnRenamed('assetid', 'asset_id')
      df_op = df_op.filter(col("asset_id").isin(self.asset_ids))
      df_op = df_op.orderBy('asset_id', col('RecordDateTime').asc())
      window = Window.partitionBy().orderBy("asset_id")
      self.df_ops = df_op.withColumn('stand_length', func.lag(col('start_bit_depth'), offset = -1).over(window) - col('start_bit_depth'))        
      self.df_ops = self.df_ops.withColumn('end_time', func.lag(col('start_time'), offset = -1).over(window))
      self.df_ops = self.df_ops.filter((col('stand_length')<self.max_stand_length) & (col('stand_length') >self.min_stand_length))
      self.df_ops = self.df_ops.withColumn('stand_id', monotonically_increasing_id())
      return self.df_ops

    
    def getWitsTable(self):
        self.df_wits = (spark.table(self.corva_wits_table))
        self.df_wits = (self.df_wits.select("asset_id", "API", "WellName", "RecordDateTime", "bit_depth", 
                                 "hole_depth", "diff_press", "rop", "rotary_rpm", "rotary_torque", 
                                 "weight_on_bit", "state" )
                       .filter(col("asset_id").isin(self.asset_ids)))
        self.df_wits = self.df_wits.withColumnRenamed('WellName', 'well_name')
        self.df_wits = self.df_wits.withColumnRenamed('API', 'api_num')
        self.df_wits = self.df_wits.orderBy('asset_id', col('RecordDateTime').asc())
        return self.df_wits
      
      
    def joinWitsOpsTable(self):
            
        # This is an important cell, joining data based on wits time within op time
        #  op---w========w---op
        #  ----------------------> Time
        self.df_wits.createOrReplaceTempView("wits")
        self.df_ops.createOrReplaceTempView("op")
        self.df = spark.sql("""
                  SELECT * from wits as w
                  LEFT JOIN op ON w.asset_id == op.asset_id
                  WHERE ((w.RecordDateTime > op.start_time) and (w.RecordDateTime < op.end_time))
                   """)
        self.df = self.df.select("w.asset_id", 'w.api_num', "w.well_name", 'op.stand_id', 'w.RecordDateTime', 'w.bit_depth', 'w.hole_depth', 'w.rop', 'w.weight_on_bit', 'w.diff_press', 'w.rotary_rpm', 'w.rotary_torque', 'w.state')
        self.df = self.df.orderBy('well_name', col('RecordDateTime').asc())
        return self.df
      
      
    def apply(self):
      self.getAllAssetIDs()
      self.getMasterTable()
      self.getOpsTable()
      self.getWitsTable()
      self.joinWitsOpsTable()
      if self.save:
        self.writeToDBFS()
        self.createDeltaTable()
    
    def getIngestTable(self):
      return self.df
    
    def getDeltaTableName(self):
      return self.write_table_name
        
    def writeToDBFS(self):
      print('start writing to dbfs')
      self.df.write.format(self.write_format).\
          mode(self.write_mode).\
          option("overwriteSchema", "true").\
          save(self.mnt_location)
      print('finish writing to dbfs')

    def createDeltaTable(self):
      try:
        drop_table_command =f'DROP TABLE {self.write_table_location}.{self.write_table_name}'
        spark.sql(drop_table_command)
      except:
        print('Table not exist')
      print('Start creating the table')
      create_table_command = f'''
      CREATE TABLE {self.write_table_location}.{self.write_table_name}
      USING DELTA LOCATION '{self.mnt_location}'
      '''
      spark.sql(create_table_command)
      print('Finish creating the table')

# COMMAND ----------

well_names = [
              'BdC-29(h)', 
              'BdC-45(h) (Aislacion)', 
              'Messenger 1-17H ST01', 
              'BdC-28(h)', 
              'BdC-30(h)', 
              'BdC-32(h)', 
              'Alma 1-12H13X24', 
              'BdC-47(h)', 
              'BdC-50(h)(I)', 
              'Miller 1-28H20X17X8R',
              'Fish 1-35H26X23',
              'Tara 1-2H11X14'
             ]

si = StandIngestion(well_names = well_names, save=False)
si.apply()

# COMMAND ----------

df=si.getIngestTable()
df.select("well_name","asset_id").distinct().show()
