''' Streaming music sources and libaries '''

from math import ceil
from datetime import datetime
from base64 import b64encode, urlsafe_b64encode
from time import sleep
from urllib import parse

import requests
from requests.auth import HTTPBasicAuth
import six
from spotipy import Spotify, SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler
from pandas import DataFrame
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from common.calling import Caller, Printer
from common.words import Texter
from common.secret import get_secret, get_token, save_token
from common.locations import SPOTIFY_AUTH_URL #, SPOTIFY_REDIRECT_URI
from common.structure import SPOTIFY_TOKENS_FOLDER, SPOTIFY_AUTHS_FOLDER, SPOTIFY_REDIRECT_URI, get_scope
from library.stripping import RemoveWords

class DSP:
    def get_albums(self, **kwargs):
        return None, None, None

    def get_favorites(self, **kwargs):
        return None, None, None
    
    def get_playlists(self, **kwargs):
        return None, None, None

class Service(Printer, Caller):
    def __init__(self):
        Printer.__init__(self)
        Caller.__init__(self)
        
    def connect(self):
        pass

    def disconnect(self):
        pass

class Spotter(DSP, Service):
    login_url = 'https://accounts.spotify.com/api'
    api_limit = 50 #100
    
    vinyl_word = 'vinyl'
    max_artists_on_album = 4
    max_tracks_album_playlist = 30
    
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
            self.add_text(f'{response.status_code}: {response.reason}')
               
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
            
            # # response = requests.post(url = f'{SPOTIFY_AUTH_URL}/api/token',
            # #                          data = {'grant_type': 'refresh_token',})

            token_info = self.get_token(f'{SPOTIFY_AUTH_URL}/api/token',
                                        refresh_token=refresh_token,
                                        data=data, headers=headers)

            if token_info:
                # # access_token = token_info['access_token']
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
        print('getting albums data')
        total = None
        offset = 0
        
        # set up placeholders for results
        info_1 = {}
        info_2 = {}
        info_3 = {}
        
        self.show_progress(offset, total)
        while total is None or offset < total:
            results = self.sp.current_user_saved_albums(limit=50, offset=offset)
            info__1 = {}
            info__2 = {}
            info__3 = {}
            
            total = results['total']
            items = results['items']
            
            info__1['artist_uris'] = [[artist['id'] for artist in item['album']['artists']] for item in items]
            info__1['album_uri'] = [item['album']['id'] for item in items]
            info__1['album_name'] = [item['album']['name'] for item in items]
            info__1['image_src'] = [item['album']['images'][0]['url'] for item in items]
            info__1['album_type'] = [item['album']['album_type'] for item in items]
            info__1['track_uris'] = [[track['id'] for track in item['album']['tracks']['items']] for item in items]
            info__1['album_duration'] = [sum(round(track['duration_ms']/(1000*60), 4) for track in item['album']['tracks']['items']) \
                                       for item in items]
            ##info__1['replacement'] = ['US' not in item['album']['available_markets'] for item in items]
            info__1['upc'] = [item['album']['external_ids']['upc'] for item in items]
            info__1['release_date'] = [self.convert_release_date(item['album']['release_date'],
                                                                 item['album']['release_date_precision']) for item in items]

            info__2['artist_uri'] = [artist['id'] for item in items for artist in item['album']['artists']]
            info__2['artist_name'] = [artist['name'] for item in items for artist in item['album']['artists']]

            info__3['album_uri'] = info__1['album_uri']
            info__3['like_date'] = [item['added_at'] for item in items]
            
            for i_0, i__0 in zip([info_1, info_2, info_3], [info__1, info__2, info__3]):
                for key in i__0.keys():
                    i_0[key] = i_0.get(key, []) + i__0[key]
                                                                        
            offset += len(items)
            self.show_progress(offset, total)

        albums_df = DataFrame(info_1)
        artists_df = DataFrame(info_2).drop_duplicates()
        ownerships_df = DataFrame(info_3)
        
        return albums_df, artists_df, ownerships_df
               
    def get_tracks_data(self, tracks_df):
        print('getting track data')
        tracks_df = self.get_tracks_info(tracks_df['track_uri'].to_list())
        
        return tracks_df
    
    def get_tracks_info(self, track_uris):
        total_rows = len(track_uris)
        info_0 = {}
        max_rows = ceil(total_rows/self.api_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            track_uris__0 = track_uris[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__0 = {}
            
            results = self.sp.tracks(track_uris__0)
            info__0['track_uri'] = track_uris__0  
            info__0['track_name'] = [t['name'] for t in results['tracks']]
            info__0['artist_uris'] = [[a['id'] for a in t['artists']] for t in results['tracks']]
            info__0['isrc'] = [t['external_ids']['isrc'] for t in results['tracks']]
            info__0['track_duration'] = [round(t['duration_ms']/(1000*60), 4) for t in results['tracks']]
            info__0['explicit'] = [t['explicit'] for t in results['tracks']]
                        
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]

        self.show_progress(max_rows, max_rows)
        tracks_df = DataFrame(info_0)
        
        return tracks_df

    def get_soundtracks_data(self, tracks_df):
        print('getting soundtrack data')
        tracks_df = self.get_soundtrack_info(tracks_df['track_uri'].to_list())
        return tracks_df
    
    def get_soundtrack_info(self, track_uris):
        total_rows = len(track_uris)
        info_0 = {}
        max_rows = ceil(total_rows/self.api_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            track_uris__0 = track_uris[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__0 = {}
            
            results = self.sp.audio_features(track_uris__0)
            info__0['track_uri'] = track_uris__0
            info__0['instrumentalness'] = [t['instrumentalness'] if t else 0 for t in results]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]

        self.show_progress(max_rows, max_rows)
        tracks_df = DataFrame(info_0)
        
        return tracks_df
    
    def get_artists_data(self, artists_df):
        print('getting artists data')
        tracks_df = self.get_artists_info(artists_df['artist_uri'].to_list())
        
        return tracks_df
    
    def get_artists_info(self, artist_uris):
        total_rows = len(artist_uris)
        info_0 = {}
        max_rows = ceil(total_rows/self.api_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            artist_uris__0 = artist_uris[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__0 = {}
            
            results = self.sp.artists(artist_uris__0)
            info__0['artist_uri'] = artist_uris__0
            info__0['artist_name'] = [a['name'] for a in results['artists']]
            info__0['genres'] = [a['genres'] for a in results['artists']]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]

        self.show_progress(max_rows, max_rows)
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
    
    def get_playlists(self, various_artist_uri=None):
        print('getting playlists data')
        total = None
        offset = 0
        
        # set up placeholders for results
        possible_lists = []
        
        self.show_progress(offset, total)
        while total is None or offset < total:
            results = self.sp.current_user_playlists(limit=50, offset=offset)
            
            total = results['total']
            items = results['items']

            possible_lists += [p['id'] for p in items if self.vinyl_word.lower() in p['name'].lower()]
            
            offset += len(items)
            self.show_progress(offset, total)
            
        if len(possible_lists):
            info_1 = {}
            info_2 = {}
            info_3 = {}
            for p_list in possible_lists:
                info__1 = {}
                info__2 = {}
                info__3 = {}
                
                results = self.sp.playlist(p_list)
                info__1['album_uri'] = [p_list]
                info__1['album_name'] = [results['name']]
                info__1['album_type'] = [results['type']]
                info__1['image_src'] = [results['images'][0]['url']]
                info__1['replacement'] = [False]
                info__1['upc'] = [None]
                
                results = self.sp.playlist_items(p_list)
                items = results['items']
                if len(items) > self.max_tracks_album_playlist:
                    # this is multiple albums on one playlist
                    pass
                
                else:
                    # this is one playlist
                    info__1['track_uris'] = [[t['track']['id'] for t in items]]
                    artist_uris = [[a['id'] for a in t['track']['artists']] for t in items]
                    info__1['artist_uris'] =  [artist_uris] if len(artist_uris) <= self.max_artists_on_album else [[various_artist_uri]]
                    info__1['album_duration'] = [sum(t['track']['duration_ms'] for t in items)/(1000*60)]
                    info__1['release_date'] = [max(self.convert_release_date(t['track']['album']['release_date'],
                                                                             t['track']['album']['release_date_precision']) \
                                                                                 for t in items)]
                    info__2['artist_uri'] = [a['id'] for t in items for a in t['track']['artists']]
                    info__2['artist_name'] = [a['name'] for t in items for a in t['track']['artists']]

                    info__3['album_uri'] = [p_list]
                    info__3['like_date'] = [min(t['added_at'] for t in items)]
                
                    for i_0, i__0 in zip([info_1, info_2, info_3], [info__1, info__2, info__3]):
                        for key in i__0.keys():
                            i_0[key] = i_0.get(key, []) + i__0[key]
                                                                        
        albums_df = DataFrame(info_1)
        artists_df = DataFrame(info_2).drop_duplicates()
        ownerships_df = DataFrame(info_3)
                
        return albums_df, artists_df, ownerships_df
    

class Sounder(DSP, Service):
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
        

class MusicBrainer(Service, Texter):
    url = 'https://musicbrainz.org/ws/2' ## should pull from locations.py
    data = {'fmt': 'json'}
    api_rate_limit = 1 # calls per second

    rid_limit = 100
    
    album_types = ['soundtrack', 'compilation', 'album', 'ep', 'live', 'remix']
    non_album_types = ['single']
    
    remove_words = [{'position': 'start',
                     'words': ['Remastered', 'Spotify Exclusive', '.*Anniversary Edition', 'Special.*Edition']},
                    {'position': 'end',
                     'words': ['Deluxe', 'Deluxe Edition', 'Deluxe Version', 'Remaster', 'Remastered', 'Standard Edition']}]
                    
    def __init__(self):
        super().__init__()
       
    def call_mb(self, term, query):
        try:
            response = requests.get(f'{self.url}/{term}', params=dict({'query': query}, **self.data), timeout=10)
            sleep(1 / self.api_rate_limit)
            if response.ok:
                return response
            else:
                self.display_status(response)
                return self.no_response()
            
        except requests.exceptions.RequestException as e:
            print(f'Request failed: {e}')
            return self.no_response()

    def get_recordings_data(self, tracks_df):
        print('getting recordings data')
        tracks_df = self.get_recordings_info(tracks_df['isrc'].to_list())
        
        return tracks_df

    def get_recordings_info(self, isrcs):
        total_rows = len(isrcs)
        info_0 = {}
        for i, isrc in enumerate(isrcs):
            isrcs__0 = [isrc] 
            self.show_progress(i + 1, total_rows, message=f'ISRC: {isrc}')
            
            info__0 = {}
            
            info__0['isrc'] = isrcs__0
            info__0['iswc'] = [self.get_iswc(isrc) for isrc in isrcs__0]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]
                
        tracks_df = DataFrame(info_0)
        
        return tracks_df
    
    def get_works_data(self, tracks_df):
        print('getting works data')
        tracks_df = self.get_works_info(tracks_df['iswc'].to_list())
        
        return tracks_df
    
    def get_works_info(self, iswcs):
        total_rows = len(iswcs)
        info_0 = {}
        for i, iswc in enumerate(iswcs):
            iswcs__0 = [iswc] 
            self.show_progress(i + 1, total_rows, message=f'ISWC: {iswc}')
            
            info__0 = {}
            
            info__0['iswc'] = iswcs__0
            info__0['release_year'] = [self.get_first_release_year(iswc) for iswc in iswcs__0]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]
                
        tracks_df = DataFrame(info_0)
        
        return tracks_df
       
    def get_iswc(self, isrc):
        iswc = None

        response = self.call_mb('recording', f'isrc:{isrc}')
        if response.ok:
            recordings = response.json()['recordings']
            if len(recordings):
                rid = response.json()['recordings'][0]['id']
                response = self.call_mb('work', f'rid:{rid}')                
                if response.ok:
                    works = response.json()['works']
                    if len(works):
                        wid = response.json()['works'][0]['id']
                        response = self.call_mb('work', f'wid:{wid}')
                        if response.ok:
                            iswc = response.json()['works'][0].get('iswcs', [None])[0]

        return iswc

    def get_release_years(self, iswc):
        release_years = []
        response = self.call_mb('work', f'iswc:{iswc}')
        if response.ok:
            rids = [w['recording']['id'] for w in response.json()['works'][0]['relations'] if w['type'] == 'performance']
            rid_ors = (' OR ').join(f'"{r}"' for r in rids[0:min(self.rid_limit, len(rids))]) # stay within uri limit
            response = self.call_mb('recording', f'rid:({rid_ors})')
            if response.ok:
                release_years = [int(r.get('first-release-date')[0:4]) for r in response.json()['recordings'] \
                                    if r.get('first-release-date')]

        return release_years
    
    def get_first_release_year(self, iswc):
        release_years = self.get_release_years(iswc)
        first_release_year = min(release_years) if release_years else None
    
        return first_release_year 
    
    def get_barcodes_data(self, albums_df):
        print('getting barcodes data')
        albums_df = self.get_barcodes_info(albums_df['upc'].to_list(),
                                           albums_df['artist_name'].to_list(),
                                           albums_df['album_name'].to_list(),
                                           albums_df['release_date'].to_list())
        
        return albums_df
    
    def get_barcodes_info(self, upcs, artist_names=None, album_names=None, release_dates=None):
        total_rows = len(upcs)
        info_0 = {}
        for i, upc in enumerate(upcs):
            self.show_progress(i, total_rows, message=f'UPC: {upc}')
            
            info__0 = {}
            
            info__0['upc'] = [upc]
            
            release_type = None           
            for kwargs in [{'upc': upc}, # barcode
                           {'artist_name': artist_names[i], 'album_name': album_names[i]}, # artist name and album title
                           {'alias_name': artist_names[i], 'album_name': album_names[i]}, # artist name and album title
                           {'artist_name': artist_names[i], 'release_date': release_dates[i]}, # release by artist on date
                           {'album_name': album_names[i], 'release_date': release_dates[i]}, # release on date with title
                           ]:
                release_type = self.get_release_type(**kwargs)
                if release_type:
                    break

            info__0['release_type'] = [release_type]
            
            for key in info__0.keys():
                info_0[key] = info_0.get(key, []) + info__0[key]
                
        self.show_progress(total_rows, total_rows)
        albums_df = DataFrame(info_0)
        
        return albums_df
    
    def get_release_type(self, upc=None, artist_name=None, alias_name=None, album_name=None, release_date=None):
        release_type = None
        query = ''
        
        if upc:
            # barcode
            query = f'barcode:"{upc}"'

        else:    
            # combination of artist, title and release date   
            queries = []
            if alias_name:
                aliases = self.get_aliases(alias_name)
                if aliases:
                    artist_name = aliases[0]
            
            if artist_name:
                queries.append(f'artist:"{artist_name}"')
            if album_name:
                title, _ = self.remove_parentheticals(album_name.replace(" - ", " ").replace("-", ""), RemoveWords.albums)
                queries.append(f'title:"{title}"')
            if release_date:
                queries.append(f'date:{release_date.strftime("%Y-%m-%d")}')
            
            if len(queries) >= 2:
                # enough information to search
                query = ' AND '.join(queries)
            
        if query:
            not_queries = ' OR '.join(f'"{t}"' for t in self.non_album_types)
            query += f' AND NOT primarytype:({not_queries})'
            release_types = []
        
            response = self.call_mb('release', f'{query}')
            if response.ok:
                releases = response.json()['releases']
                if len(releases):
                    release_groups = releases[0]['release-group']
                    release_types = []
                    if 'secondary-types' in release_groups:
                        release_types += [rt.lower() for rt in release_groups['secondary-types']]
                    elif 'primary-type' in release_groups:
                        release_types += [release_groups['primary-type'].lower()]
                
            release_type = next((a for a in self.album_types if a in release_types), None)
            self.add_text(f'release types: {release_types}')

        return release_type
    
    def get_aliases(self, artist_name):
        aliases = []
        response = self.call_mb('artist', f'alias:"{artist_name}"')
        if response.ok:
            aliases = [artist['name'] for artist in response.json()['artists'] if artist['name'] != artist_name]
            
        return aliases
        
Services = {'Spotify': Spotter,
            'SoundCloud': Sounder,
            }