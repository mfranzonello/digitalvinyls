''' Streaming music sources and libaries '''

from math import ceil
from datetime import datetime, timedelta
from base64 import b64encode, urlsafe_b64encode
from time import sleep
from urllib import parse
import itertools
import statistics
from collections import Counter
import os
from glob import glob

import requests
from requests.auth import HTTPBasicAuth
import six
from spotipy import Spotify, SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler
from pandas import DataFrame, read_csv, concat
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from billboard import ChartData

from common.calling import Caller, Printer
from common.words import Texter
from common.secret import get_secret, get_token, save_token
from common.locations import SPOTIFY_AUTH_URL, MUSICBRAINZ_URL #, SPOTIFY_REDIRECT_URI
from common.structure import SPOTIFY_TOKENS_FOLDER, SPOTIFY_AUTHS_FOLDER, SPOTIFY_REDIRECT_URI, CRITICS_FOLDER, get_scope
from library.wordbank import RemoveWords

class DSP:
    def get_albums(self, **kwargs):
        return None, None, None

    def get_favorites(self, **kwargs):
        return None, None, None
    
    def get_playlists(self, **kwargs):
        return None, None, None

class Service(Printer, Caller):
    name = None
    
    api_rate_limit = 1
    def __init__(self):
        Printer.__init__(self)
        Caller.__init__(self)
        
    def connect(self):
        pass

    def disconnect(self):
        pass
    
    def sleep(self):
        sleep(1/self.api_rate_limit)


class Spotter(DSP, Service):
    name = 'Spotify'
    
    login_url = 'https://accounts.spotify.com/api'
    api_limit = 50 #100
    api_rate_limit = 3 # calls per second
    
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
        
    ''' extract saved albums '''
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
            self.sleep(1/self.api_rate_limit)
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
    
    # # def get_albums_info(self):
    # #     return albums_df, artists_df, ownerships_df
    
    ''' extract saved playlists '''
    def get_playlists(self, various_artist_uri=None):
        albums_df, artists_df, ownerships_df = self.get_playlists_data(various_artist_uri)
        
        return albums_df, artists_df, ownerships_df
        
    def get_favorites(self, various_artist_uri=None):
        pass
        
        #return albums_df, artists_df, ownerships_df
        
    def get_playlists_data(self, various_artist_uri=None):
        print('getting playlists data')
        total = None
        offset = 0
        
        # set up placeholders for results
        possible_lists = []
        
        self.show_progress(offset, total)
        while total is None or offset < total:
            results = self.sp.current_user_playlists(limit=50, offset=offset)
            self.sleep()
            
            total = results['total']
            items = results['items']

            possible_lists += [(p['id'], count) for p in items if (count:=self.is_vinyl_playlist(p['name']))]
            
            offset += len(items)
            self.show_progress(offset, total)
            
        if len(possible_lists):
            albums_df, artists_df, ownerships_df = self.get_playlist_info(self, possible_lists, various_artist_uri)
                
        return albums_df, artists_df, ownerships_df
    
    def get_playlist_info(self, possible_lists, various_artist_uri):
        info_1 = {}
        info_2 = {}
        info_3 = {}
        for p_list, count in possible_lists:
            info__1 = {}
            info__2 = {}
            info__3 = {}
                
            results = self.sp.playlist(p_list)
            self.sleep()
            
            p_uri = p_list
            p_name = results['name']
            p_type = results['type']
            p_image_src = [results['images'][0]['url']]
                
            total = None
            offset = 0
            all_items = []
            while total is None or offset < total:
                results = self.sp.playlist_items(p_list, limit=50, offset=offset)                
                self.sleep()
            
                total = results['total']
                all_items.extend(results['items'])

                offset += len(items)
                            
            if (count == 'single') and (len(all_items) > self.max_tracks_album_playlist):
                # this is not an acceptible configuration
                index_splits = None
                
            elif count == 'multiple':
                # this is multiple albums on one playlist
                album_uris = [t['track']['album']['id'] for t in all_items]
                index_splits = self.split_by_uri(album_uris)
                    
            else:
                # this is one playlist
                index_splits = (0, len(album_uris))
                
            if index_splits:
                for i, split in enumerate(index_splits):
                    items = all_items[split[0]:split[1]]
                    
                    match count:
                        case 'multiple':
                            album_uri = f'{p_uri}|{i}'
                            album_uris = [t['track']['album']['id'] for t in items]
                            dominant_idx = self.group_by_uri(album_uris)
                            if dominant_idx:
                                # this is from one album
                                main_album = items[dominant_idx]['track']['album']
                                album_name = main_album['name']
                                album_type = main_album['type']
                                image_src = main_album['images'][0]['url']
                                upc = None #main_album[]
                            else:
                                # this is from multiple albums
                                album_name = 'Unknown Album'
                                album_type = p_type
                                image_src = p_image_src
                                upc = None
                            
                        case 'single':
                            album_uri = p_uri
                            album_name = p_name
                            album_type = p_type
                            image_src = p_image_src
                            upc = None
                            
                    # albums
                    info__1['album_uri'] = [album_uri]
                    info__1['album_name'] = [album_name]
                    info__1['album_type'] = [album_type]
                    info__1['upc'] = [upc]
                    info__1['image_src'] = [image_src]
                                
                    info__1['track_uris'] = [[t['track']['id'] for t in items]]
                    artist_uris = [[a['id'] for a in t['track']['artists']] for t in items]
                    info__1['artist_uris'] =  [artist_uris] if len(artist_uris) <= self.max_artists_on_album else [[various_artist_uri]]
                    info__1['album_duration'] = [sum(t['track']['duration_ms'] for t in items)/(1000*60)]
                    info__1['release_date'] = [max(self.convert_release_date(t['track']['album']['release_date'],
                                                                             t['track']['album']['release_date_precision']) \
                                                                                    for t in items)]
                    
                    # artists
                    info__2['artist_uri'] = [a['id'] for t in items for a in t['track']['artists']]
                    info__2['artist_name'] = [a['name'] for t in items for a in t['track']['artists']]

                    # ownerships
                    info__3['album_uri'] = [album_uri]
                    info__3['like_date'] = [min(t['added_at'] for t in items)]
                
                    for i_0, i__0 in zip([info_1, info_2, info_3], [info__1, info__2, info__3]):
                        for key in i__0.keys():
                            i_0[key] = i_0.get(key, []) + i__0[key]
                                                                        
        albums_df = DataFrame(info_1)
        artists_df = DataFrame(info_2).drop_duplicates()
        ownerships_df = DataFrame(info_3)

        return albums_df, artists_df, ownerships_df
    
    def is_vinyl_playlist(self, playlist_name):
        if self.vinyl_word.lower() in playlist_name.lower():
            if self.vinyl_word.lower() + 's' in playlist_name.lower():
                is_vinyl = 'multiple'
            else:
                is_vinyl = 'single'
        else:
            is_vinyl = False
        return is_vinyl
    
    def split_by_uri(self, uris, consecutive=4):
        index_splits = []
        current_sublist = []
        start_idx = 0
    
        for _, group in itertools.groupby(uris):
            group_list = list(group)
            end_idx = start_idx + len(group_list) - 1
        
            if len(group_list) >= consecutive:
                if current_sublist:
                    index_splits.append((current_sublist[0], current_sublist[-1]))
                    current_sublist = []
                index_splits.append((start_idx, end_idx))
            else:
                current_sublist.extend(range(start_idx, end_idx + 1))
        
            start_idx = end_idx + 1
    
        if current_sublist:
            index_splits.append((current_sublist[0], current_sublist[-1]))
    
        return index_splits

    def group_by_uri(self, uris):
        # check to see if there is a dominant uri or if they're individuals
        # find the mode
        mode_uri = statistics.mode(uris)

        # count occurrences using Counter
        counter = Counter(uris)
        if counter[mode_uri]/len(uris) > 0.5:
            uri_idx = uris.index(mode_uri)
        else:
            uri_idx = None
            
        return uri_idx

    ''' extract other fields '''
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
            self.sleep()
            
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
            self.sleep()
            
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
            self.sleep()
            
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
        

class Sounder(DSP, Service):
    name = 'SoundCloud'
    
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
    name = 'MusicBrainz'
    
    url = MUSICBRAINZ_URL
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
            self.sleep()
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
    

class BBer(Service):
    name = 'Billboard'
    
    chart_start = datetime(1963, 1, 5) # the first Billboard 200 chart
    def __init__(self):
        super().__init__()
        self.api_rate_limit = 3
           
    def get_billboard_albums(self, start_date, end_date, limit=500):
        restrictions = start_date and end_date
        i_range = range((datetime.today() - self.chart_start).days // 7 + 1)
        chart_range = [(self.chart_start + timedelta(days=i*7)) for i in i_range]
        date_range = [d.strftime('%Y-%m-%d') for d in chart_range if (not restrictions) or not (start_date <= d.date() <= end_date)]        
        if limit:
            date_range = date_range[-limit:]
                                                                        
        chart_data = []
        total_weeks = len(date_range)
        if total_weeks:
            print('getting billboard chart data')
            for c, chart_date in enumerate(date_range):
                self.show_progress(c, total_weeks, message=f'Billboard 200 for {chart_date}')
                chart = ChartData('billboard-200', date=chart_date)
                self.sleep()
                for i in range(200):
                    chart_data.append([chart.date, chart[i].peakPos, chart[i].artist, self.strip_title(chart[i].title)])
            self.show_progress(total_weeks, total_weeks)
                
            charts_df = DataFrame(chart_data, columns=['week', 'peak_position', 'credit_names', 'album_title'])
        else:
            charts_df = DataFrame()
            
        return charts_df
    
    def get_peak_positions(self, charts_df):
        peaks_df = charts_df.groupby(['credit_names', 'album_title'])[['peak_position']].min().reset_index()
        start_date = charts_df['week'].min()
        end_date = charts_df['week'].max()
        return peaks_df, start_date, end_date
    
    def strip_title(self, title):
        add_ons = ['soundtrack', 'ep']
        for add_on in add_ons:
            add = f'({add_on})'
            if title.lower()[-len(add):] == add:
                title = title[:-len(' ' + add)]
        return title


class Critic(Service):
    name = 'Best Albums Rankings'

    folder = CRITICS_FOLDER
    extension = 'csv'
    
    def __init__(self):
        super().__init__()
        
    def get_critic_files(self, excludes=DataFrame()):
        existing = excludes.apply(lambda x: self.make_file_name(x['critic_name'], x['list_year']),
                                  axis=1).values if not excludes.empty else []
        critic_files = [critic_name_year for f in glob(os.path.join(self.folder, f'*.{self.extension}')) \
                        if (critic_name_year:=os.path.splitext(os.path.basename(f))[0]) not in existing]
        return critic_files

    def get_critic_lists(self, excludes=None):
        critic_files = self.get_critic_files(excludes)
             
        if len(critic_files):
            print('getting critics picks')
            critic_lists = []
            total_files = len(critic_files)
            for i, critic_file in enumerate(critic_files):
                critic_name, list_year = self.get_name_and_year(critic_file)
                self.show_progress(i, total_files, message=f'{critic_name} {list_year}')
                critic_lists.append(self.get_critic_list(critic_file))
            self.show_progress(total_files, total_files)
            lists_df = concat(critic_lists)
        else:
            lists_df = DataFrame()
            
        return lists_df
    
    def get_critic_list(self, file_name):
        file_path = f'{self.folder}/{file_name}.{self.extension}'
        critic_name, list_year = self.get_name_and_year(file_name)
        critic_list = read_csv(file_path)
        critic_list.loc[:, 'artist_names'] = critic_list['artist_names'].apply(lambda x: x.split('; '))
        critic_list.loc[:, ['critic_name', 'list_year']] = [critic_name, list_year]
        return critic_list
    
    def get_name_and_year(self, file_name):
        critic_name = file_name[:-5].replace('_', ' ').title()
        list_year = int(file_name[-4:])
        return critic_name, list_year
    
    def make_file_name(self, critic_name, list_year):
        file_name = critic_name.lower().replace(' ', '_') + f'_{list_year}'
        return file_name