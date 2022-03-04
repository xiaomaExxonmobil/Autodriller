# Databricks notebook source
# This script is attached to a job that executes nightly @ 3 am.
# There are several variables encoded below that impact the results of the script

# SINK_DELTA_TABLE (Cmd 3, line 2): str, where the data gets sunk to so you can look at it in spotfire
# hours (Cmd 9, line 3): int, number of hours to pull from each well, currently set to 24
# window_size (Cmd 12, line 11): float, number of seconds over which to calculate rolling statistics
# lower_z_threshold (Cmd 13, line 2): float, boudary for Z-score health (0-lower_z_threshold is good health), currently 1
# upper_z_threshold (Cmd 13, line 3): float, boundary for Z-score health (lower_z_threshold-upper_z_threshold is ok), currently 2
# bad_health (Cmd 13, line 7): float, during resampling from 1s to coarser time resolution, percentage of data over upper_z_threshold to constitute bad health, currently .08
# ok_health (Cmd 13, line 8): float, during resampling from 1s to coarser time resolution, percentage of ok AND bad data to constitute ok health, currently .5
# resample frequency (Cmd 13, lines 11, 12, 14, 16, 18): str, frequency to resample data to, currently '5T', 5 minutes

# COMMAND ----------

from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.window import Window
from pyspark.sql import functions as func

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import databricks.koalas as ks


# COMMAND ----------

# Sink the data created here to this table:
SINK_DELTA_TABLE = 'auto_driller_mvp_fools_gold'

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

well_names = well_names[:5]

# COMMAND ----------

well_names

# COMMAND ----------

df_mast = (spark.table("03_corva.corva_master_assets"))
df_mast = df_mast.select("name", "latitude", "longitude")
df_mast = df_mast.filter(col("name").isin(well_names))
df_mast = df_mast.withColumnRenamed('name', 'well_name')

# COMMAND ----------

df_wits_ops = spark.table("sandbox.auto_driller_1s_silver")

# COMMAND ----------

df_wits_ops.createOrReplaceTempView("wits")
df_mast.createOrReplaceTempView("mast")
df = spark.sql("""
          SELECT * from wits as w
          LEFT JOIN mast ON w.well_name == mast.well_name
           """)
df = df.select("w.asset_id", "w.well_name", 'w.stand_id', 'w.RecordDateTime', 'w.bit_depth', 'w.hole_depth', 'w.rop', 'w.weight_on_bit', 'w.diff_press', 'w.rotary_rpm', 'w.rotary_torque', 'w.state', 'mast.latitude', 'mast.longitude')
df = df.orderBy('well_name', col('RecordDateTime').asc())

# COMMAND ----------

pddf = df.toPandas()

# COMMAND ----------

class StandStats(object):
    
    def __init__(self, df):
        self.df = df.copy()
        self.level_dict = {0: 'Low', '1' : 'Medium', '2': 'High'}
        
    def get_df(self):
        return self.df
        
    def remove_transition_data(self, margin_width=1.0):
        """
        Based on "In Slips" mode hole depth to determine the transition depth that need to be removed.
        """
        mask_slip = self.df['state'] == 'In Slips'
        in_slip_hole_depths = self.df[mask_slip]['hole_depth'].unique()
        self.df['InTransition'] = False
        for depth in in_slip_hole_depths:
            tail_filter = ((self.df['bit_depth'] < depth + margin_width) & (self.df['bit_depth'] > depth - margin_width))
            self.df.loc[tail_filter, 'InTransition'] = True
        self.df = self.df[self.df['InTransition'] == False]
        
    def cal_stand_rolling_stats(self, column='rop', window_size=30):
        grouper = self.df.groupby('stand_id')
        self.df[column + '_rolling_' + 'mean'] = grouper[column].transform(lambda x: x.rolling(window=window_size, center=True).mean())
        self.df[column + '_rolling_' + 'std'] = grouper[column].transform(lambda x: x.rolling(window=window_size, center=True).std())
        self.df[column + '_rolling_' + 'q1'] = grouper[column].transform(lambda x: x.rolling(window=window_size, center=True).quantile(0.25))
        self.df[column + '_rolling_' + 'q3'] = grouper[column].transform(lambda x: x.rolling(window=window_size, center=True).quantile(0.75))
        self.df[column + '_rolling_' + 'max']= grouper[column].transform(lambda x: x.rolling(window=window_size, center=True).max())
        self.df[column + '_rolling_' + 'min']= grouper[column].transform(lambda x: x.rolling(window=window_size, center=True).min())
        
        self.df[column + '_rolling_' + 'iqr'] =self.df[column + '_rolling_' + 'q3'] - self.df[column + '_rolling_' + 'q1']
        self.column = column
        self.window_size = window_size
        
    def cal_outlier_bound(self, thres_hold=1.0):
        self.df[self.column + '_upper'] = self.df[self.column + '_rolling_' + 'q3'] +\
                                          thres_hold * self.df[self.column + '_rolling_' + 'iqr']
        self.df[self.column + '_lower'] = self.df[self.column + '_rolling_' + 'q1'] -\
                                          thres_hold * self.df[self.column + '_rolling_' + 'iqr']
        self.mask = (self.df[self.column] > self.df[self.column + '_upper' ]) | (self.df[self.column] <  self.df[self.column + '_lower'])
        self.df.loc[:, 'is_outlier'] = self.mask
        
        
    
    def _cal_stand_duration(self):
        stand_duration = pd.to_datetime(self.df.groupby('stand_id')['RecordDateTime'].max()) - pd.to_datetime(self.df.groupby('stand_id')['RecordDateTime'].min())
        stand_duration = stand_duration.astype('timedelta64[m]')        
        return stand_duration 

def get_health(series):
    
    lower_z_threshold = 10
    upper_z_threshold = 20
    
    bins = [0, lower_z_threshold, upper_z_threshold, 100]
    labels = ['good', 'ok', 'bad']
    
#     bad_health = .08 # these values represent % of chunk with bad / ok / good label
#     ok_health = .5
    
    bad_health = 8.0 # these values represent % of chunk with bad / ok / good label
    ok_health = 3.0
    
    # all this function does is map out values in the passed series to bins specified above
    binned = pd.cut(series, bins=bins, labels=labels, right=False).value_counts(normalize=True)
    if binned.loc['bad'] > bad_health: # WATCH OUT! hard coded values here (globalize these?)
        health = 'bad'
    elif binned.loc['bad'] + binned.loc['ok'] > ok_health:
        health = 'ok'
    else:
        health = 'good'
    return health

def handle_string_data(series):
    # returns most frequently occurring string in passed series
    if len(series.value_counts()) > 0:
        return series.value_counts().index[0]
    else:
        return 'NAN'

# COMMAND ----------

# Pick which variables you care about
analyze_these_vars = [
    'rop', 
    'weight_on_bit', 
    'diff_press', 
    'rotary_rpm', 
    'rotary_torque'
]

# Pick the size of the window you want to average (etc.) over; default is 30 s
window_size = 30 # seconds (or rows)

# We're going to loop over each well and put the results into a list, 
# where each element in the list is a pd.DataFrame
# Then we'll concatenate all the elements in the list together and make one massive
# pd.DataFrame that we'll write out to a delta table, and you can look at different wells
# The more wells you throw into this loop the longer it will take (it runs serially)
# So if you wish to scale up to 100-1000 wells (I believe) it will take ~ 4hrs
# Something smarter could be done to process these in parallel or just append results to 
# previously obtained results in the sql WRITES in the last cell
all_the_wells = []
this_well = None
# Loop over each well
for name in well_names:
    print(f'Working on {name}... Calculating rolling stats for:')
    this_well = pddf[pddf.well_name==name].sort_values('RecordDateTime')
    SS_object = StandStats(this_well)
    SS_object.remove_transition_data()    
    # Then loop over each variable
    for variable in analyze_these_vars:
        print(f'{variable}')
        SS_object.cal_stand_rolling_stats(column=variable, window_size=window_size)        
    # Overwrite dataframe this_well
    this_well = SS_object.get_df()
    this_well['stand_duration'] = this_well.stand_id.map(SS_object._cal_stand_duration())
    # And we'll ad Z-score into the mix
    print(f'     Calculating IQR for:')
    for variable in analyze_these_vars:
        thres_hold = 1.0
        print(f'          {variable}')
        this_well['upperBound_' + variable] = this_well[variable + '_rolling_' + 'q3'] +\
                                          thres_hold * this_well[variable + '_rolling_' + 'iqr']
        this_well['lowerBound_' + variable] = this_well[variable + '_rolling_' + 'q3'] -\
                                                  thres_hold * this_well[variable + '_rolling_' + 'iqr']
        this_well['isOutlier_' + variable] = ((this_well[variable] > this_well['upperBound_' + variable])|
                                              (this_well[variable] <this_well['lowerBound_' + variable]))
    this_well.fillna(0, inplace=True)
    this_well.replace([-np.inf, np.inf], 0, inplace=True)
    all_the_wells.append(this_well)
    
# This smushes all the wells together
all_the_wells = pd.concat(all_the_wells).reset_index(drop=True)

# COMMAND ----------

all_the_wells.groupby()

# COMMAND ----------

zcols = [col for col in all_the_wells.columns if 'isOutlier_' in col]
print(zcols)
wells = []
series = None
for well in well_names:
    print(well)
    this_well = all_the_wells[all_the_wells.well_name==well]
    new_df = pd.DataFrame()
    for col in this_well.columns:
        if col in zcols:
            print(col)
            col.split()
            variable = col[10:]
            low_level_bound = 8.0
            high_level_bound = 20.0
            outlier_p = 'outlier_percent_' +variable
            is_outlier_v = 'isOutlier_' + variable
            new_df[outlier_p] =this_well[this_well[is_outlier_v]==True].groupby('stand_id').count()[is_outlier_v]/this_well.groupby('stand_id').count()[is_outlier_v]*100
            
            new_df['health_' + variable] = np.where(((new_df[outlier_p]<high_level_bound)&
                                                              (new_df[outlier_p]>low_level_bound)), 
                                                             'ok','good')
            mask = new_df[outlier_p] > high_level_bound
            new_df.loc[mask, 'health_' + variable]='bad'  
    stand_grouper = this_well.groupby('stand_id')
    new_df['asset_id'] = this_well['asset_id'].loc[0]
    new_df['well_name'] = this_well['well_name'].loc[0]
    new_df['stand_id'] = stand_grouper['stand_id'].mean()
    new_df['start_bitdepth'] = stand_grouper['bit_depth'].min()
    new_df['end_bitdepth'] = stand_grouper['bit_depth'].max()
    new_df['stand_duration'] =stand_grouper['stand_duration'].mean()
    wells.append(new_df)

wells = pd.concat(wells).reset_index(drop=True)

# COMMAND ----------

wells

# COMMAND ----------

# This is the final vote across the 5 variables for total health (call to mode in line 3)
all_cols = [col for col in wells.columns if 'health' in col]
vote = wells[all_cols].mode(axis=1)[0]
wells['health_total'] = vote
wells.dropna(inplace=True)
# This was because spark was unhappy with columns that contained mixed data types (double and string)
# Force-casting any object-type columns into entirely string
for col in wells.columns:
    if wells[col].dtype=='O':
        wells[col] = wells[col].astype(str)

# COMMAND ----------

wells.columns

# COMMAND ----------

well_name = 'BdC-45(h) (Aislacion)'
mask_final = (wells['well_name']==well_name)
col = 'rop'
plt.subplot(2,1,1)
x = 0.5*(wells[mask_final&bit_depth_mask].start_bitdepth + wells[mask_final&bit_depth_mask].end_bitdepth)*0.3048
sns.scatterplot(wells[mask_final]['stand_id'], wells[mask_final]['stand_duration'],hue = wells[mask_final]['health_total'], palette={'good':'green', 'ok':'orange', 'bad':'red'})

# COMMAND ----------

# #-----------------------------------------------------------------------------------------------------------
# df_final_to_spark = spark.createDataFrame(wells)
# #-----------------------------------------------------------------------------------------------------------
# #database credentials
# serverName = "jdbc:sqlserver://sqls-wells-ussc-prd.database.windows.net:1433"
# databaseName = "WellsIntelGoldTableDev"
# url = serverName + ";" + "databaseName=" + databaseName + ";"

# databricksKeyVaultScope = "Wells.Databricks.Keyvault.Secrets"

# userName = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksUsername")
# password = dbutils.secrets.get(databricksKeyVaultScope, "wellsIntelGoldTableDatabricksPassword")

# #write to Azure SQL
# try:
#     df_final_to_spark.write \
#         .format("com.microsoft.sqlserver.jdbc.spark") \
#         .mode("overwrite") \
#         .option("url", url) \
#         .option("dbtable", SINK_DELTA_TABLE) \
#         .option("user", userName) \
#         .option("password", password) \
#         .save()
# except ValueError as error :
#     print("Connector write failed", error)
