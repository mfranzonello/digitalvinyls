from math import ceil
from datetime import datetime
from base64 import b64encode, urlsafe_b64encode
from time import sleep

import requests
from requests.auth import HTTPBasicAuth
import six
from spotipy import Spotify, SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler
from pandas import DataFrame, concat
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from common.calling import Caller
from common.secret import get_secret, get_token, save_token
from common.locations import SPOTIFY_AUTH_URL #, SPOTIFY_REDIRECT_URI
from common.structure import SPOTIFY_TOKENS_FOLDER, SPOTIFY_AUTHS_FOLDER, SPOTIFY_REDIRECT_URI, get_scope

class DSP:
    def __init__(self):
        pass

    def get_albums(self):
        return None, None, None

    def get_favorites(self):
        return None, None, None
    
    def get_playlists(self):
        return None, None, None

class Service(Caller, DSP):
    def __init__(self):
        super().__init__()
        
    def connect(self):
        pass

    def disconnect(self):
        pass

class Spotter(Service):
    login_url = 'https://accounts.spotify.com/api'
    api_limit = 50 #100
    
    vinyl_word = 'vinyl'
    
    def __init__(self):
        super().__init__()
        self.sp = None
        self.user_id = None
        
        self.scope = get_scope(SPOTIFY_AUTHS_FOLDER)
        
    def get_auth_header(self):
        auth_header = urlsafe_b64encode((get_secret('SPOTIFY_CLIENT_ID') + ':' + get_secret('SPOTIFY_CLIENT_SECRET')).encode())
        
        headers = {
                  'Content-Type': 'application/x-www-form-urlencoded',
                  'Authorization': f'Basic {auth_header}' #'.decode("ascii")}'
                  }
        return headers #auth_header
    
    def get_access(self, user_id):
        code = 'AQBozGvN-ipEs0fselrTIbcfqb23msKnSm0hUC1ePBlmZXKAO-vKBKvriEXnY5t7lZEAS_zFxmRlREIi7O7WA1CtsOR5KnHeUSDw6n7OBEGfvEW5DWv8Jry4OpeuGwNERnlyZy32KgM4hn4vB55D4bj_BjCcTX0lEljtpjH4IGkJlvmDNiXj9imO2sGOvNvrikZiY-L0oKNQ4GYtXhnXvb2nKL7p-Vkft19s4lewHwMHGT4VgaFzMzzDHwyCb5JXyc3O9VLurgDatnCbI3GndlZuH9c_eeJd4AWGMQeHXmKu4LsfFymH1R7bEJa7aXdYxtpX4I5hZeE'
        #get_token(SPOTIFY_TOKENS_FOLDER, user_id)['code']
        url = f'{self.login_url}/token'
        data = {'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': SPOTIFY_REDIRECT_URI}
        response = requests.post(url, data=data, headers=self.get_auth_header())
        # # print(url)
        # # print(data)
        
        if response.ok:
            token_info = {item: response.json()[item] for item in ['access_token' 'refresh_token', 'scopes']}
            print(f'TOKEN: {token_info}')
            save_token(SPOTIFY_TOKENS_FOLDER, user_id, token_info)
            
        else:
            print(f'{response.status_code} - {response.reason}')
               
    def connect(self, user_id=None):
        client_credentials_manager = None
        auth_manager = None
        cache_handler = None
        self.user_id = user_id

        client_id = get_secret('SPOTIFY_CLIENT_ID')
        client_secret = get_secret('SPOTIFY_CLIENT_SECRET')
        
        if self.user_id:
            refresh_token = get_token(SPOTIFY_TOKENS_FOLDER, user_id)['refresh_token']
           
            data = {'grant_type': 'refresh_token',
                    'refresh_token': refresh_token,
                    }
            auth_header = b64encode(six.text_type(client_id + ':' + client_secret).encode('ascii'))
            headers = {'Authorization': f'Basic {auth_header.decode("ascii")}'}
            
            response = requests.post(url = f'{SPOTIFY_AUTH_URL}/api/token',
                                     data = {'grant_type': 'refresh_token',})

            token_info = self.get_token(f'{SPOTIFY_AUTH_URL}/api/token',
                                        refresh_token=refresh_token,
                                        data=data, headers=headers)

            if token_info:
                access_token = token_info['access_token']
                cache_handler = MemoryCacheHandler(token_info)

            auth_manager = SpotifyOAuth(client_id=client_id,
                                        client_secret=client_secret,
                                        redirect_uri=SPOTIFY_REDIRECT_URI,
                                        cache_handler=cache_handler,
                                        open_browser=False,
                                        scope=self.scope)
        else:
            client_credentials_manager = SpotifyClientCredentials(client_id=client_id,
                                                                  client_secret=client_secret)
            
        self.sp = Spotify(client_credentials_manager=client_credentials_manager,
                          auth_manager=auth_manager)
        
    def get_albums(self):
        total = None
        offset = 0
        
        # set up placeholders for results
        info_1 = {}
        info_2 = {}
        info_3 = {}
        
        while total is None or offset < total:
            results = self.sp.current_user_saved_albums(limit=50, offset=offset)
            info__1 = {}
            info__2 = {}
            info__3 = {}
            
            total = results['total']
            items = results['items']
            
            info__1['artist_ids'] = [[artist['id'] for artist in item['album']['artists']] for item in items]
            info__1['album_id'] = [item['album']['id'] for item in items]
            info__1['album_name'] = [item['album']['name'] for item in items]
            info__1['image_src'] = [item['album']['images'][0]['url'] for item in items]
            info__1['album_type'] = [item['album']['album_type'] for item in items]
            info__1['track_list'] = [[track['id'] for track in item['album']['tracks']['items']] for item in items]
            info__1['album_duration'] = [sum(round(track['duration_ms']/(1000*60), 4) for track in item['album']['tracks']['items']) \
                                       for item in items]
            info__1['replacement'] = ['US' not in item['album']['available_markets'] for item in items]
            info__1['upc'] = [item['album']['external_ids']['upc'] for item in items]
            info__1['release_date'] = [self.convert_release_date(item['album']['release_date'],
                                                                 item['album']['release_date_precision']) for item in items]

            info__2['artist_id'] = [artist['id'] for item in items for artist in item['album']['artists']]
            info__2['artist_name'] = [artist['name'] for item in items for artist in item['album']['artists']]

            info__3['album_id'] = info__1['album_id']
            info__3['like_date'] = [item['added_at'] for item in items]
            
            for i_0, i__0 in zip([info_1, info_2, info_3], [info__1, info__2, info__3]):
                for key in i__0.keys():
                    i_0[key] = i_0.get(key, []) + i__0[key]
                                                                        
            offset += len(items)
            print(f'{offset} / {total}')

        albums_df = DataFrame(info_1)
        artists_df = DataFrame(info_2).drop_duplicates()
        ownerships_df = DataFrame(info_3)
        
        return albums_df, artists_df, ownerships_df
               
    def get_tracks_data(self, tracks_df):
        print('getting track data')
        tracks_df = self.get_tracks_info(tracks_df['track_id'].to_list())
        
        return tracks_df
    
    def get_tracks_info(self, track_ids):
        total_rows = len(track_ids)
        info_0 = {}
        for i in range(ceil(total_rows/self.api_limit)):
            track_ids__0 = track_ids[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__0 = {}
            
            results = self.sp.tracks(track_ids__0)
            info__0['track_id'] = track_ids__0  
            info__0['track_name'] = [t['name'] for t in results['tracks']]
            info__0['artist_ids'] = [[a['id'] for a in t['artists']] for t in results['tracks']]
            info__0['isrc'] = [t['external_ids']['isrc'] for t in results['tracks']]
            info__0['track_duration'] = [round(t['duration_ms']/(1000*60), 4) for t in results['tracks']]
            info__0['explict'] = [t['explicit'] for t in results['tracks']]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]

        tracks_df = DataFrame(info_0)
        
        return tracks_df

    def get_soundtracks_data(self, tracks_df):
        print('getting soundtrack data')
        tracks_df = self.get_soundtrack_info(tracks_df['track_id'].to_list())
        return tracks_df
    
    def get_soundtrack_info(self, track_ids):
        total_rows = len(track_ids)
        info_0 = {}
        for i in range(ceil(total_rows/self.api_limit)):
            track_ids__0 = track_ids[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__0 = {}
            
            results = self.sp.audio_features(track_ids__0)
            info__0['track_id'] = track_ids__0
            info__0['instrumentalness'] = [t['instrumentalness'] if t else 0 for t in results]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]

        tracks_df = DataFrame(info_0)
        
        return tracks_df
    
    def get_artists_data(self, artists_df):
        print('getting artists data')
        tracks_df = self.get_artists_info(artists_df['artist_id'].to_list())
        
        return tracks_df
    
    def get_artists_info(self, artist_ids):
        total_rows = len(artist_ids)
        info_0 = {}
        for i in range(ceil(total_rows/self.api_limit)):
            artist_ids__0 = artist_ids[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__0 = {}
            
            results = self.sp.artists(artist_ids__0)
            info__0['artist_id'] = artist_ids__0
            info__0['artist_name'] = [a['name'] for a in results['artists']]
            info__0['genres'] = [a['genres'] for a in results['artists']]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]

        artists_df = DataFrame(info_0)
        
        return artists_df

    def convert_release_date(self, r_date, release_date_precision):
        match release_date_precision:
            case 'year':
                date_format = '%Y'
            case 'month':
                date_format = '%Y-%m'
            case 'day':
                date_format = '%Y-%m-%d'        
        release_date = datetime.strptime(r_date, date_format)
        
        return release_date
    
    def get_playlists(self):
        total = None
        offset = 0
        
        # set up placeholders for results
        possible_lists = []
        
        while total is None or offset < total:
            results = self.sp.current_user_playlists(limit=50, offset=offset)
            
            total = results['total']
            items = results['items']

            names = [p['name'] for p in items]
            print(f'Names: {names}')
            possible_lists += [p['id'] for p in items if self.vinyl_word.lower() in p['name'].lower()]
            
            offset += len(items)
            print(f'{offset} / {total}')
            
        if len(possible_lists):
            info_0 = {}
            for p_list in possible_lists:
                info__0 = {}
                results = self.sp.playlist_items(p_list)
                #input(results['items'])
                info__0['album_id'] = results['id']
                info__0['image_src'] = results['images'][0]['url']
                info__0['album_name'] = results['name']
                info__0['album_type'] = results['type']
                #info__0[]
        
        print(f'Possible lists: {possible_lists}')

        # # info__0 = {}
        # # info__0['album_id'] = [p['id'] for p in results['items']]
        # # info__0['image_src'] = [p['images'][0]['url'] for p in results['items']]
        # # info__0['album_name'] = [p['name'] for p in results['items']]
        # # results['items'][0]['id']

        albums_df = None
        artists_df = None
        ownerships_df = None
        return albums_df, artists_df, ownerships_df
    
    # OLD FUNCTIONS
    # # def get_track_elements(self, uri):
    # #     results = self.sp.track(uri)
        
    # #     elements = {'uri': results['uri'],
    # #                 'name': results['name'],
    # #                 'artist_uri': [artist['uri'] for artist in results['artists']],
    # #                 'album_uri': results['album']['uri'],
    # #                 'explicit': results['explicit'],
    # #                 'popularity': results['popularity'],
    # #                 }
    # #     return elements

    # # def get_artist_elements(self, uri):
    # #     results = self.sp.artist(uri)

    # #     elements = {'uri': results['uri'],
    # #                 'name': results['name'],
    # #                 'genres': results['genres'],
    # #                 'popularity': results['popularity'],
    # #                 'followers': results['followers']['total'],
    # #                 'src': self.get_image_src(results['images'], name=results['name']),
    # #                 }
    # #     return elements

    # # def get_album_elements(self, uri):
    # #     results = self.sp.album(uri)

    # #     elements = {'uri': results['uri'],
    # #                 'name': results['name'],
    # #                 'genres': results['genres'],
    # #                 'popularity': results['popularity'],
    # #                 'release_date': self.get_date(results['release_date'], results['release_date_precision']),
    # #                 'src': self.get_image_src(results['images'], name=results['name']),
    # #                 }
    # #     return elements

    # # def get_image_src(self, result_images, name=None):
    # #     if len(result_images):
    # #         # get src from Spotify
    # #         src = result_images[0]['url']

    # #     elif name:
    # #         # get first result from Google
    # #         src = self.gimager.get_image_src(name)

    # #     else:
    # #         # return null
    # #         src = None

    # #     return src

    # # def search_for_track(self, artist, title):
    # #     results = self.sp.search(q=f'artist: {artist} track: {title}', type='track')
    # #     first_result = results['tracks']['items'][0]

    # #     uris = {'track': first_result['uri'],
    # #             'artists': [artist['uri'] for artist in first_result['artists']],
    # #             'album': first_result['album']['uri'],
    # #             }
    # #     return uris

    # # def get_user_elements(self, user):
    # #     results = self.sp.user(user)

    # #     elements = {'uri': results['uri'],
    # #                 'followers': results['followers']['total'],
    # #                 'src': self.get_image_src(results['images']),
    # #                 }
    # #     return elements

    # # def get_audio_features(self, uri):
    # #     results = self.sp.audio_features(uri)

    # #     features = {key: results[0][key] for key in self.audio_features}
    # #     features['duration'] = results[0]['duration_ms'] / 1000 / 60

    # #     return features

class Sounder(Service):
    def __init__(self):
        super().__init__()
        self.username = None
        self.driver = None

    def get_user_auths(self, username):
        self.username = username
        
    def connect(self, username=None):
        self.get_user_auths(username)
        
        chrome_options = Options()
        chrome_options.add_argument('--headless=new')
        self.driver = webdriver.Chrome(options=chrome_options)

    def disconnect(self):
        self.driver.quit()
        
    def get_albums(self):
        return None, None, None
    
        # # url = f'https://soundcloud.com/{self.username}'
        
        # # for page in ['likes', 'albums']:
        # #     page_length = 0
        # #     self.driver.get(f'{url}/{page}')
        # #     elem = self.driver.find_element(By.TAG_NAME, 'body')
            
        # #     while len(self.driver.page_source) > page_length:
        # #         page_length = len(self.driver.page_source)
        # #         elem.send_keys(Keys.PAGE_DOWN)
        # #         sleep(0.2)
        
        # #     elems = self.driver.find_elements(By.CLASS_NAME, 'soundList__item')
        # #     print(len(elems))

        # # return

    def get_playlists(self):
        return None, None, None

    def get_favorites(self):
        return None, None, None    
        

class MusicBrainer:
    url = 'https://musicbrainz.org/ws/2'
    data = {'fmt': 'json'}
    api_rate_limit = 1 # calls per second
    calls_per_isrc = 4
    calls_per_upc = 1
    rid_limit = 100
    
    album_types = ['compilation', 'album', 'single']

    def __init__(self):
        pass
    
    def connect(self):
        pass
    
    def disconnect(self):
        pass
    
    def get_compilations_data(self, tracks_df):
        print('getting compilations data')
        tracks_df = self.get_compilations_info(tracks_df['isrc'].to_list())
        
        return tracks_df
    
    def get_compilations_info(self, isrcs):
        # # total_rows = len(isrcs)
        info_0 = {}
        for isrc in isrcs:
        # # for i in range(ceil(total_rows/self.api_limit)):
            isrcs__0 = [isrc] #isrcs[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            print(f'ISRCs: {isrcs__0}')
            info__0 = {}
            
            info__0['isrc'] = isrcs__0
            info__0['release_year'] = [self.get_first_release_year(isrc) for isrc in isrcs__0]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]
                
            sleep(self.api_rate_limit * self.calls_per_isrc)

        tracks_df = DataFrame(info_0)
        
        return tracks_df
    
    def get_iswc(self, isrc):
        iswc = None

        response = requests.get(f'{self.url}/recording/?query=isrc:{isrc}', data=self.data)
        recordings = response.json()['recordings']
        if len(recordings):
            rid = response.json()['recordings'][0]['id']
            response = requests.get(f'{self.url}/work/?query=rid:{rid}', data=self.data)
            works = response.json()['works']
            if len(works):
                wid = response.json()['works'][0]['id']
                response = requests.get(f'{self.url}/work/?query=wid:{wid}', data=self.data)
                iswc = response.json()['works'][0]

        return iswc

    def get_release_years(self, iswc):
        if iswc:
            rids = [w['recording']['id'] for w in iswc['relations'] if w['type'] == 'performance']
            rid_ors = '%20OR%20rid:'.join(rids[0:min(self.rid_limit, len(rids))]) # stay within uri limit
            response = requests.get(f'{self.url}/recording/?query=rid:{rid_ors}', data=self.data)
            release_years = [int(r.get('first-release-date')[0:4]) for r in response.json()['recordings'] if r.get('first-release-date')]
        
        else:
            release_years = []

        return release_years
    
    def get_first_release_year(self, isrc):
        iswc = self.get_iswc(isrc)
        if iswc:
            release_years = self.get_release_years(iswc)
            first_release_year = min(release_years)
        else:
            first_release_year = None

        return first_release_year 
    
    def get_barcodes_data(self, albums_df):
        print('getting categories data')
        albums_df = self.get_barcodes_info(albums_df['upc'].to_list())
        
        return albums_df
    
    def get_barcodes_info(self, upcs):
        # # total_rows = len(isrcs)
        info_0 = {}
        for upc in upcs:
        # # for i in range(ceil(total_rows/self.api_limit)):
            upcs__0 = [upc] #isrcs[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            print(f'UPCs: {upcs__0}')
            info__0 = {}
            
            info__0['upc'] = upcs__0
            info__0['release_type'] = [self.get_release_type(upc) for upc in upcs__0]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]
                
            sleep(self.api_rate_limit * self.calls_per_upc)

        albums_df = DataFrame(info_0)
        
        return albums_df
    
    def get_release_type(self, upc):
        release_types = []
        response = requests.get(f'{self.url}/release/?query=barcode:{upc}', data=self.data)
        if response.ok:
            releases = response.json()['releases']
            if len(releases):
                release_groups = releases[0]['release-group']
                release_types = []
                if 'secondary-types' in release_groups:
                    release_types += [rt for rt in release_groups['secondary-types']]
                elif 'primary-type' in release_groups:
                    release_types += release_groups['primary-type']
                
        release_type = next((a for a in self.album_types if a in [r.lower() for r in release_types]), None)
        
        return release_type
        


Services = {'Spotify': Spotter,
            'SoundCloud': Sounder,
            }