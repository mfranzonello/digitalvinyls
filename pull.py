''' Get data to build library '''

from setup import set_up_database, set_up_user
from music.dsp import Spotter, Sounder


def update_albums(neon, DSPs, user):
    for S in DSPs:
        service = S()
        if user.has_service(service.name):
            service.connect(user.get_user_id(service.name))
            service_id = neon.get_service_id(service.name)
            sources_df = neon.get_sources(service_id)
            
            for source_id, source_name in sources_df.values:
                match source_name:
                    case 'albums':
                        albums_df, artists_df, ownerships_df = service.get_albums()
                    case 'playlists':
                        various_artist_uri = neon.get_various_artist_uri(service_id)
                        albums_df, artists_df, ownerships_df = service.get_playlists(various_artist_uri)
                    case 'favorites':
                        albums_df, artists_df, ownerships_df = service.get_favorites()
                    case _:
                        albums_df = artists_df = ownerships_df = None
                update_pulls(neon, albums_df, artists_df, ownerships_df, service_id, source_id, user)
            service.disconnect()
        
def update_pulls(neon, albums_df, artists_df, ownerships_df, service_id, source_id, user):
    if artists_df is not None:
        neon.update_artists(artists_df, service_id)
    if albums_df is not None:
        neon.update_albums(albums_df, source_id)
    if ownerships_df is not None:
        neon.update_ownerships(ownerships_df, source_id, user.user_id)

if __name__ == '__main__':
    DSPs = [Spotter, Sounder]
    neon = set_up_database()
    user_ids = neon.get_user_ids()
    
    for user_id in user_ids:
        user = set_up_user(neon, user_id)
        update_albums(neon, DSPs, user)
          
    quit()