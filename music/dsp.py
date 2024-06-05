from datetime import datetime
from base64 import b64encode
from time import sleep

import requests
import six
from spotipy import Spotify, SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler
from pandas import DataFrame
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from common.calling import Caller
from common.secret import get_secret
from common.locations import SPOTIFY_AUTH_URL, SPOTIFY_REDIRECT
from common.structure import get_token, get_scope


class Spotter(Caller):
    min_track_count = 4
    
    def __init__(self):
        super().__init__()
        self.sp = None
        self.user_id = None
        
        self.scope = get_scope()
        
    def get_user_auths(self, user_id):
        user_auths = get_token(user_id)
        
        return user_auths
        
    def connect_to_service(self, user_id, auth=True):
        client_credentials_manager = None
        auth_manager = None
        cache_handler = None
        self.user_id = user_id

        client_id = get_secret('SPOTIFY_CLIENT_ID')
        client_secret = get_secret('SPOTIFY_CLIENT_SECRET')

        if auth:
            user_auths = self.get_user_auths(user_id)
            refresh_token = user_auths['refresh_token']
           
            data = {'grant_type': 'refresh_token',
                    'refresh_token': refresh_token,
                    }
            auth_header = b64encode(six.text_type(client_id + ':' + client_secret).encode('ascii'))
            headers = {'Authorization': f'Basic {auth_header.decode("ascii")}'}

            token_info = self.get_token(f'{SPOTIFY_AUTH_URL}/api/token',
                                        refresh_token=refresh_token,
                                        data=data, headers=headers)

            if token_info:
                access_token = token_info['access_token']
                cache_handler = MemoryCacheHandler(token_info)

            auth_manager = SpotifyOAuth(client_id=client_id,
                                        client_secret=client_secret,
                                        redirect_uri=SPOTIFY_REDIRECT,
                                        cache_handler=cache_handler,
                                        open_browser=False,
                                        scope=self.scope)
        else:
            client_credentials_manager = SpotifyClientCredentials(client_id=client_id,
                                                                  client_secret=client_secret)
            
        self.sp = Spotify(client_credentials_manager=client_credentials_manager,
                          auth_manager=auth_manager)
        
    def get_liked_albums(self):
        total = None
        offset = 0
        
        # set up placeholders for results
        albums_df = DataFrame(columns = ['album_id', 'artist_id', 'album_name', 'album_type', 
                                         'genres', 'categorization', 'release_date',
                                         'image_src', 'track_list', 'skip_list', 'replacement'
                                         ])
        
        artists_df = DataFrame(columns = ['artist_id', 'artist_name'])
        
        likes_df = DataFrame(columns = ['album_id', 'like_date'])

        while total is None or offset < total:
            results = self.sp.current_user_saved_albums(limit=50, offset=offset)

            total = results['total']
            
            for item in results['items']:
                album = item['album']
                # get artist information
                for artist in album['artists']:
                    if artist['id'] not in artists_df['artist_id']:
                        artists_df.loc[len(artists_df)] = [artist['id'], artist['name']] 
                artist_id = album['artists'][0]['id'] ## should just capture all of artist, or ID
                artist_name = album['artists'][0]['name']
                    
                # album information
                album_id = album['id']
                album_name = album['name']
                image_src = album['images'][0]['url']
                    
                # album information - genre and category
                album_type = album['album_type']
                genres = album['genres']
                    
                if album_type == 'single':
                    if album['total_tracks'] >= self.min_track_count:
                        categorization = 'ep' 
                    else:
                        categorization = 'single'
                elif album_type == 'compilation':
                    categorization = 'compilation'
                elif 'score' in genres:
                    categorization = 'score'
                elif 'covers' in genres:
                    categorization = 'covers'
                elif 'parody' in genres:
                    categorization = 'parody'
                elif 'soundtrack' in genres:
                    categorization = 'soundtrack'
                else:
                    categorization = 'studio'
                        
                track_list = [track['id'] for track in album['tracks']['items']]
                skip_list = None # need to know hidden tracks
                replacement = 'US' not in album['available_markets'] # filled in later
                
                # album information - release date information
                r_date = album['release_date']
                match album['release_date_precision']:
                    case 'year':
                        release_date = datetime(int(r_date), 1, 1)
                    case 'month':
                        release_date = datetime(int(r_date[0:4]), int(r_date[5:]), 1)
                    case 'day':
                        release_date = r_date
                albums_df.loc[len(albums_df)] = [album_id, artist_id, album_name, album_type,
                                                    genres, categorization, release_date, image_src,
                                                    track_list, skip_list, replacement]
                    
                # like information
                like_date = item['added_at']
                likes_df.loc[len(likes_df)] = [album_id, like_date]
                                                         
            offset = len(albums_df)
            print(f'{offset} / {total}')
            
        return albums_df, artists_df, likes_df

    def get_track_elements(self, uri):
        results = self.sp.track(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'artist_uri': [artist['uri'] for artist in results['artists']],
                    'album_uri': results['album']['uri'],
                    'explicit': results['explicit'],
                    'popularity': results['popularity'],
                    }
        return elements

    def get_artist_elements(self, uri):
        results = self.sp.artist(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'genres': results['genres'],
                    'popularity': results['popularity'],
                    'followers': results['followers']['total'],
                    'src': self.get_image_src(results['images'], name=results['name']),
                    }
        return elements

    def get_album_elements(self, uri):
        results = self.sp.album(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'genres': results['genres'],
                    'popularity': results['popularity'],
                    'release_date': self.get_date(results['release_date'], results['release_date_precision']),
                    'src': self.get_image_src(results['images'], name=results['name']),
                    }
        return elements

    def get_date(self, string, precision):
        pattern = {'year': '%Y',
                   'month': '%Y-%m',
                   'day': '%Y-%m-%d'}[precision]
        
        date = datetime.strptime(string, pattern)
            
        return date

    def get_image_src(self, result_images, name=None):
        if len(result_images):
            # get src from Spotify
            src = result_images[0]['url']

        elif name:
            # get first result from Google
            src = self.gimager.get_image_src(name)

        else:
            # return null
            src = None

        return src

    def search_for_track(self, artist, title):
        results = self.sp.search(q=f'artist: {artist} track: {title}', type='track')
        first_result = results['tracks']['items'][0]

        uris = {'track': first_result['uri'],
                'artists': [artist['uri'] for artist in first_result['artists']],
                'album': first_result['album']['uri'],
                }
        return uris

    def get_user_elements(self, user):
        results = self.sp.user(user)

        elements = {'uri': results['uri'],
                    'followers': results['followers']['total'],
                    'src': self.get_image_src(results['images']),
                    }
        return elements

    def get_audio_features(self, uri):
        results = self.sp.audio_features(uri)

        features = {key: results[0][key] for key in self.audio_features}
        features['duration'] = results[0]['duration_ms'] / 1000 / 60

        return features  

class Sounder(Caller):
    def __init__(self):
        super().__init__()
        self.username = None
        self.driver = None

    def get_user_auths(self, username):
        self.username = username
        
    def connect_to_service(self, username):
        self.get_user_auths(username)
        
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')
        self.driver = webdriver.Chrome(options=chrome_options)
        
    def get_liked_albums(self):
        url = f'https://soundcloud.com/{self.username}'
        
        for page in ['likes', 'albums']:
            page_length = 0
            self.driver.get(f'{url}/{page}')
            elem = self.driver.find_element(By.TAG_NAME, 'body')
            
            while len(self.driver.page_source) > page_length:
                page_length = len(self.driver.page_source)
                elem.send_keys(Keys.PAGE_DOWN)
                sleep(0.2)
        
            elems = self.driver.find_elements(By.CLASS_NAME, 'soundList__item')
            print(len(elems))

        return
    
    def disconnect_from_service(self):
        self.driver.quit()


class Sonoser(Caller):
    port = 5005
    server = f'http://localhost:{port}'
    def __init__(self):
        super().__init__()
        self.controller = None
        self.rooms = []
        
    def get_rooms(self):
        
        response = requests.get(f'{self.url}/zones')
        if response.ok:
            zones = response.json()
            rooms = [m['roomName'] for z in zones for m in z['members']]
            coordinators = [z['coordinator']['roomName'] for z in zones]
            members = [len(z['members']) for z in zones]

            self.controller = coordinators[members.index(max(members))]
            self.rooms = rooms
            
    def group_rooms(self):
        self.get_rooms()
        for room in self.rooms:
            if room != self.controller:
                requests.get(f'{self.url}/{room}/join/{self.controller}')

    def get_favorites(self):
        response = requests.get(f'{self.url}/favorites')
        if response.ok:
            favorites = response.json()
            return favorites
            
    def play_album(self, service, album_id):
        match service:
            case 'spotify':
                requests.get(f'{self.url}/{self.controller}/spotify/now/spotify:album:{album_id}')
                
            case 'soundcloud':
                # match album to favorites playlist
                favorites = self.get_favorites()
                if favorites and album_id in favorites:
                    playlist_id = album_id
                    requests.get(f'{self.url}/{self.controller}/favorites/{playlist_id}')
