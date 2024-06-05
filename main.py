from common.data import Neon
from music.dsp import Spotter, Sounder
from music.users import User

def get_albums_from_spotify(user):
    if user.has_spotify():
        spotter = Spotter()
        spotter.connect_to_service(user.spotify_user_id)
        spotify_albums_df, spotify_artists_df, spotify_likes_df = spotter.get_liked_albums()
        
    else:
        spotify_albums_df = None
        spotify_artists_df = None
        spotify_likes_df = None

    return spotify_albums_df, spotify_artists_df, spotify_likes_df

def get_albums_from_soundcloud(user):
    if user.has_soundcloud():
        sounder = Sounder()
        sounder.connect_to_service(user.soundcloud_username)
        soundcloud_albums_df, souncloud_artists_df, soundcloud_likes_df = sounder.get_liked_albums()
        sounder.disconnect_from_service()

    else:
        soundcloud_albums_df = None
        soundcloud_artists_df = None
        soundcloud_likes_df = None
        
    return soundcloud_albums_df, soundcloud_artists_df, soundcloud_likes_df

def get_user_albums(user, service_id=None, update=False):
    neon = Neon()
    neon.connect()  
    
    get_albums = {1: get_albums_from_spotify,
                  2: get_albums_from_soundcloud}
    if update:
        for service_id in get_albums:
            albums_df, artists_df, likes_df = get_albums[service_id](user)
            if albums_df is not None:
                neon.update_artists(artists_df, service_id)
                neon.update_albums(albums_df, service_id)
                neon.update_likes(likes_df, service_id, user.user_id)
    
    retreived_albums_df = neon.get_user_albums(user)
    
    print(retreived_albums_df)
    
def set_up_database(drop=False):
    neon = Neon()
    neon.connect()
    if drop:
        neon.drop_tables()
    neon.create_tables()
    

user = User(1, 'Michael', 'Franzonello', spotify_user_id='1235496003')
#user = User('Michael', 'Franzonello', soundcloud_username='michael-franzonello')
#user = User('Michael', 'Franzonello', spotify_user_id='1235496003', soundcloud_username='michael-franzonello')

set_up_database() #drop=True
get_user_albums(user, update=True)
