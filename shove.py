''' Fill out library '''

from setup import set_up_database
from music.dsp import Services, MusicBrainer
        
def update_tracks(neon):
    for service_name in Services:
        service_id = neon.get_service_id(service_name)
        tracks_df = neon.get_tracks_to_update(service_id)
        if len(tracks_df):
            service = Services[service_name]()
            service.connect()
            tracks_df = service.get_tracks_data(tracks_df)
            service_id = neon.get_service_id(service_name)
            neon.update_tracks(tracks_df, service_id)    
            service.disconnect()
    
def update_artists(neon):
    for service_name in Services:
        service_id = neon.get_service_id(service_name)
        artists_df = neon.get_artists_to_update(service_id)
        if len(artists_df):
            service = Services[service_name]()
            service.connect()
            artists_df = service.get_artists_data(artists_df)
            neon.update_artists(artists_df, service_id)    
            service.disconnect()

def update_soundtracks(neon):
    for service_name in Services:
        service_id = neon.get_service_id(service_name)
        tracks_df = neon.get_soundtracks_to_update(service_id)
        if len(tracks_df):
            service = Services[service_name]()
            service.connect()
            tracks_df = service.get_soundtracks_data(tracks_df)
            neon.update_soundtracks(tracks_df, service_id)    
            service.disconnect()
        
def update_recordings(neon):
    # check recordings
    tracks_df = neon.get_recordings_to_update()
    if len(tracks_df):
        service = MusicBrainer()        
        service.connect()
        tracks_df = service.get_recordings_data(tracks_df)
        neon.update_recordings(tracks_df)
        service.disconnect()
        
def update_works(neon):
    # check works
    tracks_df = neon.get_works_to_update()
    if len(tracks_df):
        service = MusicBrainer()        
        service.connect()
        tracks_df = service.get_works_data(tracks_df)
        neon.update_works(tracks_df)
        service.disconnect()

def update_barcodes(neon):
    albums_df = neon.get_barcodes_to_update()
    if len(albums_df):
        service = MusicBrainer()        
        service.connect()
        albums_df = service.get_barcodes_data(albums_df)
        neon.update_barcodes(albums_df)    
        service.disconnect()

if __name__ == '__main__':
    neon = set_up_database()
        
    update_tracks(neon)
    update_artists(neon)
    update_soundtracks(neon)
    update_recordings(neon)
    update_works(neon)
    update_barcodes(neon)
    
    neon.refresh_views()

    quit()