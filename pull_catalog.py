''' Fill out catalog details from crowd-sourced content '''

from .setup import set_up_database, is_updatable
from .music.services import MusicBrainer, FMer
        
def update_recordings(neon):
    # check recordings
    tracks_df = neon.get_recordings_to_update()
    if is_updatable(tracks_df):
        service = MusicBrainer()        
        service.connect()
        tracks_df = service.get_recordings_data(tracks_df)
        if is_updatable(tracks_df):
            neon.update_recordings(tracks_df)
        service.disconnect()
        
def update_works(neon):
    # check works
    tracks_df = neon.get_works_to_update()
    if is_updatable(tracks_df):
        service = MusicBrainer()        
        service.connect()
        tracks_df = service.get_works_data(tracks_df)
        if is_updatable(tracks_df):
            neon.update_works(tracks_df)
        service.disconnect()

def update_barcodes(neon):
    albums_df = neon.get_upcs_to_update()
    if is_updatable(albums_df):
        service = MusicBrainer()
        service.connect()
        albums_df = service.find_barcodes_data(albums_df)
        if is_updatable(albums_df):
            neon.update_upcs(albums_df)
        service.disconnect()
        
    albums_df = neon.get_barcodes_to_update()
    if is_updatable(albums_df):
        service = MusicBrainer()        
        service.connect()
        albums_df = service.get_barcodes_data(albums_df)
        if is_updatable(albums_df):
            neon.update_barcodes(albums_df)    
        service.disconnect()
        
def main():
    neon = set_up_database()
     
    update_recordings(neon)
    update_works(neon)
    update_barcodes(neon)

    neon.refresh_views()

if __name__ == '__main__':
    main()    