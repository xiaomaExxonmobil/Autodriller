from StandStats import *
import pandas as pd

class WellPrediction(object):
    def __init__(self, well_name, df):
        self.df = df.filter(df.well_name == well_name).toPandas()
        self.well_name = well_name
        self.df_results = {}
        self.df_voting = pd.DataFrame()
        self.columns = []
        self.stand_stats = StandStats(self.df)

    def analysis(self, columns, thres_hold=1.5, lower_bound=5.0, high_bound=8.0, if_voting=True):
        self.stand_stats = StandStats(self.df)
        self.stand_stats.remove_transition_data()
        for column in columns:
            self.stand_stats.cal_stand_rolling_stats(column=column)
            self.stand_stats.cal_outlier_bound(thres_hold=thres_hold)
            df_result = self.stand_stats.cal_dysfunc_level(high_level_bound=high_bound, low_level_bound=lower_bound)
            self.df_results[column] = df_result
        if if_voting:
            self.df_voting = self._cal_voting_result()
            return self.df_voting
        else:
            return self.df_results

    def display_result(self, start_stand=None, end_stand=None, ylim=None, xlim=None, figsize=(12, 8), line_width=5,
                       font_scale=2, in_ft=True):
        self.stand_stats.display_dysfunc_level(self.df_voting, start_stand=0 if start_stand is None else start_stand,
                                               end_stand=self.df_voting.count()[0] if end_stand is None else end_stand,
                                               well_name=self.well_name, ylim=ylim, xlim=xlim, figsize=figsize,
                                               line_width=line_width, font_scale=font_scale, in_ft=in_ft)

    def display_stand(self, standNo, column):
        self.stand_stats.display_stand(standNo, column)

    def _cal_voting_result(self):
        df_voting = pd.DataFrame()
        for column, df in self.df_results.items():
            df_voting[column + '_' + 'dysfunc_level'] = df['dysfunc_level']
        df_voting['voting_level'] = df_voting.mode(axis=1)[0]
        move_cols = ['start_bitdepth', 'end_bitdepth', 'standDuration']
        for col in move_cols:
            df_voting[col] = self.df_results['rop'][col]
        return df_voting

    def get_df_results(self):
        return self.df_results

    def get_df(self):
        return self.df

    def get_df_standstatus(self):
        return self.stand_stats.get_df()