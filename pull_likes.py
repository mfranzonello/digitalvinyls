''' Get data to build library '''

from setup import set_up_database, set_up_user, is_updatable
from music.services import UserServices

def update_albums(neon, DSPs, user):
    for S in DSPs:
        service = S()
        if user.has_service(service.name):
            service.connect(user.get_user_id(service.name))
            various_artist_uri = neon.get_various_artist_uri(neon.get_service_id(service.name))
            service.add_various_artist_id(various_artist_uri)
            service_id = neon.get_service_id(service.name)
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
                        albums_df = artists_df = ownerships_df = None

                update_pulls(neon, albums_df, artists_df, ownerships_df, service_id, source_id, user)
            service.disconnect()
        
def update_pulls(neon, albums_df, artists_df, ownerships_df, service_id, source_id, user):
    if is_updatable(artists_df):
        neon.update_artists(artists_df, service_id)
    if is_updatable(albums_df):
        neon.update_albums(albums_df, source_id)
    if is_updatable(ownerships_df):
        neon.update_ownerships(ownerships_df, source_id, user.user_id)

def main():
    neon = set_up_database()
    user_ids = neon.get_user_ids()
    
    for user_id in user_ids:
        user = set_up_user(neon, user_id)
        update_albums(neon, UserServices, user)    

if __name__ == '__main__':
    main()