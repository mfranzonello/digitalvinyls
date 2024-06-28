''' Fill out library '''

from setup import set_up_database, is_updatable
from music.spotify import Spotter
from music.soundcloud import Sounder
from music.onedrive import Driver
from music.musicbrainz import MusicBrainer
        
def update_tracks(neon, DSPs):
    for S in DSPs:
        service = S()
        service_id = neon.get_service_id(service.name)
        tracks_df = neon.get_tracks_to_update(service_id)
        if is_updatable(tracks_df):
            service.connect()
            tracks_df = service.get_tracks_data(tracks_df)
            if is_updatable(tracks_df):
                neon.update_tracks(tracks_df, service_id)    
            service.disconnect()
    
def update_artists(neon, DSPs):
    for S in DSPs:
        service = S()
        service_id = neon.get_service_id(service.name)
        artists_df = neon.get_artists_to_update(service_id)
        if is_updatable(artists_df):
            service.connect()
            artists_df = service.get_artists_data(artists_df)
            if is_updatable(artists_df):
                neon.update_artists(artists_df, service_id)    
            service.disconnect()

def update_soundtracks(neon, DSPs):
    for S in DSPs:
        service = S()
        service_id = neon.get_service_id(service.name)
        tracks_df = neon.get_soundtracks_to_update(service_id)
        if is_updatable(tracks_df):
            service.connect()
            tracks_df = service.get_soundtracks_data(tracks_df)
            if is_updatable(tracks_df):
                neon.update_soundtracks(tracks_df, service_id)    
            service.disconnect()
        
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
            neon.update_upcs(albums_df, )
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
    
    DSPs = [Driver] #Spotter, Sounder]
     
    update_tracks(neon, DSPs)
    update_artists(neon, DSPs)
    update_soundtracks(neon, DSPs)
    update_recordings(neon)
    update_works(neon)
    update_barcodes(neon)

    neon.refresh_views()

if __name__ == '__main__':
    main()    