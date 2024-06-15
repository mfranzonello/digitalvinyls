from main import set_up_database, set_up_user
from music.dsp import Services, MusicBrainer

def update_albums(neon, user):
    for service_name in Services:
        if user.has_service(service_name):
            service = Services[service_name]()
            service.connect(user.get_user_id(service_name))
            service_id = neon.get_service_id(service_name)
            sources_df = neon.get_sources(service_id)
            
            for source_id, source_name in sources_df.values:
                match source_name:
                    case 'albums':
                        albums_df, artists_df, ownerships_df = service.get_albums()
                    case 'playlists':
                        albums_df, artists_df, ownerships_df = service.get_playlists()
                    case 'favorites':
                        albums_df, artists_df, ownerships_df = service.get_favorites()
                    case _:
                        albums_df, artists_df, ownerships_df = None, None, None
                update_pulls(neon, albums_df, artists_df, ownerships_df, service_id, source_id, user)
            service.disconnect()
        
def update_pulls(neon, albums_df, artists_df, ownerships_df, service_id, source_id, user):
    if artists_df is not None:
        neon.update_artists(artists_df, service_id)
    if albums_df is not None:
        neon.update_albums(albums_df, source_id)
    if ownerships_df is not None:
        neon.update_ownerships(ownerships_df, source_id, user.user_id)

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
        
def update_compilations(neon):
    tracks_df = neon.get_compilations_to_update()
    if len(tracks_df):
        service = MusicBrainer()        
        service.connect()
        tracks_df = service.get_compilations_data(tracks_df)
        neon.update_compilations(tracks_df)    
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
    user_ids = neon.get_user_ids()
    
    for user_id in user_ids:
        user = set_up_user(neon, user_id)
        update_albums(neon, user)
        
    update_tracks(neon)
    update_artists(neon)
    update_soundtracks(neon)
    #update_compilations(neon)
    update_barcodes(neon)
    
    quit()