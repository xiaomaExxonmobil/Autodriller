# Databricks notebook source
import pyspark.sql
from pyspark.sql import functions as func
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
from plotly.subplots import make_subplots
import plotly.express as px
import seaborn as sns

spark = pyspark.sql.session.SparkSession()

class StandIngestion():
    
    def __int__(self):
        self.save = True
        self.well_names = None
        self.corva_ops_table = '03_corva.corva_operations_connections_silver'
        self.corva_wits_table = '03_corva.corva_drilling_wits_silver'
        self.df_wits = None
        self.df_ops = None
        self.min_stand_length = 20.0
        self.max_stand_length = 100.0
        self.write_format = 'delta'
        self.write_mode = 'overwrite'
        self.write_table_name = 'auto_driller_mvp_silver'
        self.mnt_location = f'/mnt/delta/{self.write_table_name}'
        self.write_table_location = 'sandbox'


    def get_window(self):
        window = Window.partitionBy().orderBy('asset_id')
        return window
    
    
    def get_ops_table(self):
        df_op = (spark.table(self.corva_ops_table)
              .filter(col('operation_name')=="Drilling(Connection)")
              .orderBy('assetid',col('recorddatetime').asc()))
        df_op = df_op.select("assetid", "api_number", "name", "RecordDateTime", "start_bit_depth" )
        df_op = df_op.filter(col("name").isin(self.well_names))
        df_op = df_op.withColumnRenamed('name', 'well_name')
        df_op = df_op.withColumnRenamed('RecordDateTime', 'start_time')
        df_op = df_op.withColumnRenamed('assetid', 'asset_id')
        df_op = df_op.orderBy('asset_id', col('RecordDateTime').asc())
        df_op.select('well_name').distinct().show()
        
        window = Window.partitionBy().orderBy("asset_id")
        
        self.df_ops = df_op.withColumn('stand_length', func.lag(col('start_bit_depth'), offset = -1).over(window) - col('start_bit_depth'))        
        self.df_ops = self.df_ops.withColumn('end_time', func.lag(col('start_time'), offset = -1).over(window))
        self.df_ops = self.df_ops.filter((col('stand_length')<self.max_stand_length) & (col('stand_length') >self.min_stand_length))
        self.df_ops = self.df_ops.withColumn('stand_id', monotonically_increasing_id())

        return self.df_ops
    
    
    def get_wits_table(self):
        self.df_wits = (spark.table(self.corva_wits_table))
        self.df_wits = self.df_wits.select("asset_id", "API", "WellName", "RecordDateTime", "bit_depth", 
                                 "hole_depth", "diff_press", "rop", "rotary_rpm", "rotary_torque", 
                                 "weight_on_bit", "state" )
        self.df_wits = self.df_wits.filter(col("WellName").isin(self.well_names))
        self.df_wits = self.df_wits.withColumnRenamed('WellName', 'well_name')
        self.df_wits = self.df_wits.withColumnRenamed('API', 'api_num')
        self.df_wits = self.df_wits.orderBy('asset_id', col('RecordDateTime').asc())
        return self.df_wits


    def join_wits_ops(self):
            
        # This is an important cell, joining data based on wits time within op time
        #  op---w========w---op
        #  ----------------------> Time
        self.df_wits.createOrReplaceTempView("wits")
        self.df_ops.createOrReplaceTempView("op")
        df = spark.sql("""
                  SELECT * from wits as w
                  LEFT JOIN op ON w.well_name == op.well_name
                  WHERE ((w.RecordDateTime > op.start_time) and (w.RecordDateTime < op.end_time))
                   """)
        df = df.select("w.asset_id", 'w.api_num', "w.well_name", 'op.stand_id', 'w.RecordDateTime', 'w.bit_depth', 'w.hole_depth', 'w.rop', 'w.weight_on_bit', 'w.diff_press', 'w.rotary_rpm', 'w.rotary_torque', 'w.state')
        df = df.orderBy('well_name', col('RecordDateTime').asc())
        return df

    
    def write_df(self, df):
        df.write.format(self.write_format).\
            mode(self.write_mode).\
            option("overwriteSchema", "true").\
            save(self.mnt_location)

        drop_table_command = f'DROP TABLE {self.write_table_location}.{self.mnt_location}'
        spark.sql(drop_table_command)
        
        create_table_command = f'''
        CREATE TABLE {self.write_table_location}.{self.write_table_name}
        USING DELTA LOCATION {self.mnt_location}
        '''
        spark.sql(create_table_command)





