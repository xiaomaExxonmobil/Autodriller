import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
import plotly.graph_objects as go

class StandStats(object):

    def __init__(self, df):
        self.df = df.copy()
        self.level_dict = {0: 'Low', '1': 'Medium', '2': 'High'}

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
            tail_filter = (
                        (self.df['bit_depth'] < depth + margin_width) & (self.df['bit_depth'] > depth - margin_width))
            self.df.loc[tail_filter, 'InTransition'] = True
        self.df = self.df[self.df['InTransition'] == False]

    def cal_stand_rolling_stats(self, column='rop', window_size=30):
        grouper = self.df.groupby('stand_id')
        self.df[column + '_rolling_' + 'mean'] = grouper[column].transform(
            lambda x: x.rolling(window=window_size, center=True).mean())
        self.df[column + '_rolling_' + 'std'] = grouper[column].transform(
            lambda x: x.rolling(window=window_size, center=True).std())
        self.df[column + '_rolling_' + 'q1'] = grouper[column].transform(
            lambda x: x.rolling(window=window_size, center=True).quantile(0.25))
        self.df[column + '_rolling_' + 'q3'] = grouper[column].transform(
            lambda x: x.rolling(window=window_size, center=True).quantile(0.75))
        self.df[column + '_rolling_' + 'max'] = grouper[column].transform(
            lambda x: x.rolling(window=window_size, center=True).max())
        self.df[column + '_rolling_' + 'min'] = grouper[column].transform(
            lambda x: x.rolling(window=window_size, center=True).min())

        self.df[column + '_rolling_' + 'iqr'] = self.df[column + '_rolling_' + 'q3'] - self.df[
            column + '_rolling_' + 'q1']
        self.column = column
        self.window_size = window_size

    def cal_outlier_bound(self, thres_hold=1.0, method='IQR'):
        """
        Calculate the outlier boundaries:
        IQR approach: Q1/Q3 +/ thred * IQR
        Mean/STD approach : mean +/- thred * std
        """
        if method == 'IQR':
            self.df[self.column + '_upper'] = self.df[self.column + '_rolling_' + 'q3'] + thres_hold * self.df[
                self.column + '_rolling_' + 'iqr']
            self.df[self.column + '_lower'] = self.df[self.column + '_rolling_' + 'q1'] - thres_hold * self.df[
                self.column + '_rolling_' + 'iqr']
        self.mask = (self.df[self.column] > self.df[self.column + '_upper']) | (
                    self.df[self.column] < self.df[self.column + '_lower'])
        self.df.loc[:, 'is_outlier'] = self.mask

    def cal_dysfunc_level(self, high_level_bound=8.0, low_level_bound=3.0):
        df_stand_stats = pd.DataFrame({'outlier_percent':
                                           self.df[self.df['is_outlier'] == True].groupby('stand_id').count()[
                                               'is_outlier'] / self.df.groupby('stand_id').count()['is_outlier'] * 100,
                                       'num_outliers':
                                           self.df[self.df['is_outlier'] == True].groupby('stand_id').count()[
                                               'is_outlier'],
                                       'start_bitdepth': self.df.groupby('stand_id')['bit_depth'].min(),
                                       'end_bitdepth': self.df.groupby('stand_id')['bit_depth'].max(),
                                       'standDuration': self._cal_stand_duration()})
        df_stand_stats['dysfunc_level'] = np.where(((df_stand_stats['outlier_percent'] < high_level_bound) &
                                                    (df_stand_stats['outlier_percent'] > low_level_bound)), 'Medium',
                                                   'Low')
        mask = df_stand_stats['outlier_percent'] > high_level_bound
        df_stand_stats.loc[mask, 'dysfunc_level'] = 'High'
        df_stand_stats.reset_index(drop=True, inplace=True)
        mask_stand = ((df_stand_stats.end_bitdepth - df_stand_stats.start_bitdepth) > 80) & (
                    (df_stand_stats.end_bitdepth - df_stand_stats.start_bitdepth) < 110)
        df_stand_stats = df_stand_stats[mask_stand]
        df_stand_stats.reset_index(drop=True, inplace=True)
        df_stand_stats = df_stand_stats[df_stand_stats['standDuration'] < 1500]
        df_stand_stats.reset_index(drop=True, inplace=True)

        self.df_stand_status = df_stand_stats

        return self.df_stand_status

    # well_name = 'BdC-45'
    def display_dysfunc_level(self, column=None, level_column='voting_level', start_stand=10, end_stand=50,
                              well_name="Alma 1-12H13X24", ylim=None, xlim=None, figsize=(16, 8), line_width=2,
                              font_scale=1.0, in_ft=False):
        plt.style.use('seaborn')
        plt.figure(figsize=figsize)
        sns.set(font_scale=font_scale)

        df_plot_copy = self.df_stand_status.copy()
        # return df_plot_copy
        plt.plot((df_plot_copy.loc[start_stand:end_stand - 1, 'start_bitdepth']
                  + df_plot_copy.loc[start_stand:end_stand - 1, 'end_bitdepth']) / 2,
                 df_plot_copy.loc[start_stand:end_stand - 1, 'standDuration'], label=well_name)
        for i in range(start_stand, end_stand):
            start_depth = df_plot_copy.loc[i, 'start_bitdepth']
            end_depth = df_plot_copy.loc[i, 'end_bitdepth']
            stand_duration = df_plot_copy.loc[i, 'standDuration']
            level = df_plot_copy.loc[i, level_column]
            if level == 'High':
                plt.plot([start_depth, end_depth], [stand_duration, stand_duration], '-r', linewidth=line_width)
            elif level == 'Medium':
                plt.plot([start_depth, end_depth], [stand_duration, stand_duration], '-', color='orange',
                         linewidth=line_width)
            elif level == 'Low':
                plt.plot([start_depth, end_depth], [stand_duration, stand_duration], '-g', linewidth=line_width)
        if not ylim:
            plt.ylim(0)
        else:
            plt.ylim(ylim[0], ylim[1])
        if xlim:
            plt.xlim(xlim[0], xlim[1])
        plt.grid(True)
        if in_ft:
            plt.xlabel('Depth (ft)')
        else:
            plt.xlabel('Depth (m)')

        plt.ylabel('Minutes Per Stand')
        plt.legend()
        # plt.title(column.upper() + ' Variance' + ' Characterization')

    def display_stand(self, standNo, column):
        df_temp = self.df[self.df['stand_id'] == standNo]
        df_temp.loc[:, 'stand_id'] = df_temp.loc[:, 'stand_id'] - self.df['stand_id'].min()
        df_temp.reset_index(drop=True, inplace=True)

        mask_outlier = (df_temp['is_outlier'] == True)
        fig = go.Figure()

        fig.add_trace(go.Scatter(x=df_temp['RecordDateTime'],
                                 y=df_temp[column],
                                 name='RawData',
                                 mode='markers'))

        fig.add_trace(go.Scatter(x=df_temp[mask_outlier]['RecordDateTime'],
                                 y=df_temp[mask_outlier][column],
                                 name='Outliers',
                                 mode='markers'))

        fig.add_trace(go.Scatter(x=df_temp['RecordDateTime'],
                                 y=df_temp[column + '_upper'],
                                 name='UpperBound',
                                 mode='lines'))

        fig.add_trace(go.Scatter(x=df_temp['RecordDateTime'],
                                 y=df_temp[column + '_lower'],
                                 name='LowerBound',
                                 mode='lines'))

        fig.add_trace(go.Scatter(x=df_temp['RecordDateTime'],
                                 y=df_temp[column + '_rolling_' + 'max'],
                                 name='Max',
                                 mode='lines'))

        fig.add_trace(go.Scatter(x=df_temp['RecordDateTime'],
                                 y=df_temp[column + '_rolling_' + 'min'],
                                 name='Min',
                                 mode='lines'))

        # outlier_percent = self.df_stand_status.loc[standNo, 'outlier_percent']
        # title_str ='Stand-' + str(standNo+1) + " " + "{:.2f}%".format(outlier_percent) + " Outliers " +  "(" + column.upper() + " Variance" + ")"
        # fig.update_layout(height=600, width=900,title_text=title_str)
        fig.update_xaxes(title_text='Time')
        fig.update_yaxes(title_text=column)
        fig.show()

    def display_entire_data(self, stats_list=['q1', 'q3', 'mean', 'std'], well_name='BdC-45', show_outlier=False):

        mask_outlier = (self.df['is_outlier'] == True)
        fig = go.Figure()

        fig.add_trace(go.Scatter(x=self.df['RecordDateTime'],
                                 y=self.df[self.column],
                                 name='RawData',
                                 mode='lines'))
        if show_outlier:
            fig.add_trace(go.Scatter(x=self.df[mask_outlier]['RecordDateTime'],
                                     y=self.df[mask_outlier][self.column],
                                     name='Outliers',
                                     mode='markers'))
        for stat in stats_list:
            stat_name = self.column + '_rolling_' + stat

            fig.add_trace(go.Scatter(x=self.df['RecordDateTime'],
                                     y=self.df[stat_name],
                                     name='rolling_' + stat,
                                     mode='lines'))

        fig.update_layout(height=600, width=900, title_text=well_name, xaxis=dict(rangeslider=dict(visible=True)))
        fig.update_xaxes(title_text='Time')
        fig.update_yaxes(title_text=self.column)
        fig.show()

    def _cal_stand_duration(self):
        stand_duration = pd.to_datetime(self.df.groupby('stand_id')['RecordDateTime'].max()) - pd.to_datetime(
            self.df.groupby('stand_id')['RecordDateTime'].min())
        stand_duration = stand_duration.astype('timedelta64[m]')
        return stand_duration