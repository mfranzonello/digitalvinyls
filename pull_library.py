''' Fill out library details from the DSPs '''

from .setup import set_up_database, is_updatable
from .music.services import UserServices
        
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
                
def main():
    neon = set_up_database()
     
    update_tracks(neon, UserServices)
    update_artists(neon, UserServices)
    update_soundtracks(neon, UserServices)

    neon.refresh_views()

if __name__ == '__main__':
    main()    