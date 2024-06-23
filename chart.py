''' See what the general public says '''

from setup import set_up_database
from music.dsp import BBer, Critic
        
def update_charts(neon):
    service = BBer()
    start_date, end_date = neon.get_billboard_to_update()
    charts_df = service.get_billboard_albums(start_date=start_date, end_date=end_date)
    peaks_df, start_date, end_date = service.get_peak_positions(charts_df)
    neon.update_billboard(peaks_df, start_date, end_date)

def update_critics(neon):
    service = Critic()
    excludes = neon.get_critics_to_update()
    lists_df = service.get_critic_lists(excludes)
    if not lists_df.empty:
        neon.update_critics(lists_df)

if __name__ == '__main__':
    neon = set_up_database()
    
    update_critics(neon)    
    update_charts(neon)
    
    neon.refresh_views()

    quit()