''' Billboard API functions '''

from datetime import datetime, timedelta

from pandas import DataFrame
from billboard import ChartData

from ..common.structure import BILLBOARD_RATE_LIMIT
from .dsp import Service

class BBer(Service):
    name = 'Billboard'
    api_rate_limit = BILLBOARD_RATE_LIMIT
    
    chart_start = datetime(1963, 1, 5) # the first Billboard 200 chart
    def __init__(self):
        super().__init__()
           
    def get_billboard_albums(self, start_date, end_date, limit=100):
        restrictions = start_date and end_date
        i_range = range((datetime.today() - self.chart_start).days // 7 + 1)
        chart_range = [(self.chart_start + timedelta(days=i*7)) for i in i_range[::-1]]
        date_range = [d.strftime('%Y-%m-%d') for d in chart_range if (not restrictions) or not (start_date <= d.date() <= end_date)]        
        if limit:
            date_range = date_range[-limit:]
                                                                        
        chart_data = []
        total_weeks = len(date_range)
        if total_weeks:
            print('getting billboard chart data')
            for c, chart_date in enumerate(date_range):
                self.show_progress(c, total_weeks, message=f'Billboard 200 for {chart_date}')
                chart = ChartData('billboard-200', date=chart_date)
                self.sleep()
                for i in range(200):
                    chart_data.append([chart.date, chart[i].peakPos, chart[i].artist, self.strip_title(chart[i].title)])
            self.show_progress()
                
            charts_df = DataFrame(chart_data, columns=['week', 'peak_position', 'credit_names', 'album_title'])
        else:
            charts_df = DataFrame()
            
        return charts_df
    
    def get_peak_positions(self, charts_df):
        peaks_df = charts_df.groupby(['credit_names', 'album_title'])[['peak_position']].min().reset_index()
        start_date = charts_df['week'].min()
        end_date = charts_df['week'].max()
        return peaks_df, start_date, end_date
    
    def strip_title(self, title):
        add_ons = ['soundtrack', 'ep']
        for add_on in add_ons:
            add = f'({add_on})'
            if title.lower()[-len(add):] == add:
                title = title[:-len(' ' + add)]
        return title
