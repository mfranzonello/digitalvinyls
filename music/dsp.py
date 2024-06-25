''' Streaming music sources and libraries '''

from math import ceil
from datetime import datetime, timedelta
from base64 import b64encode, urlsafe_b64encode
from time import sleep
import itertools
import statistics
from collections import Counter
import os
from glob import glob
import re
from urllib import parse

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
from selenium.common.exceptions import NoSuchElementException
from billboard import ChartData

from common.calling import Caller, Printer
from common.words import Texter
from common.secret import get_secret, get_token, save_token
from common.locations import SPOTIFY_AUTH_URL, SOUNDCLOUD_PLAY_URL, SOUNDCLOUD_WIDGET_URL, MUSICBRAINZ_URL
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
    various_artist_uri = None
    
    vinyl_word = 'vinyl'
    max_artists_on_album = 4
    max_tracks_album_playlist = 30
    # # min_album_tracks = 4
    
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
        
    def announce(self, message):
        print(f'{message} from {self.name}')
        
    def add_various_artist_id(self, various_artist_uri):
        self.various_artist_uri = various_artist_uri


class Spotter(DSP, Service):
    name = 'Spotify'
    
    login_url = 'https://accounts.spotify.com/api'
    api_limit = 50 #100
    api_rate_limit = 3 # calls per second
    
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
        code = ''
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
        info_albums = {}
        info_artists = {}
        info_ownerships = {}
        
        self.show_progress(offset, total)
        while total is None or offset < total:
            results = self.sp.current_user_saved_albums(limit=50, offset=offset)
            self.sleep()
            info__albums = {}
            info__artists = {}
            info__ownerships = {}
            
            total = results['total']
            items = results['items']
            
            info__albums['artist_uris'] = [[artist['id'] for artist in item['album']['artists']] for item in items]
            info__albums['album_uri'] = [item['album']['id'] for item in items]
            info__albums['album_name'] = [item['album']['name'] for item in items]
            info__albums['image_src'] = [item['album']['images'][0]['url'] for item in items]
            info__albums['album_type'] = [item['album']['album_type'] for item in items]
            info__albums['track_uris'] = [[track['id'] for track in item['album']['tracks']['items']] for item in items]
            info__albums['album_duration'] = [sum(round(track['duration_ms']/(1000*60), 4) for track in item['album']['tracks']['items']) \
                                       for item in items]
            info__albums['upc'] = [item['album']['external_ids']['upc'] for item in items]
            info__albums['release_date'] = [self.convert_release_date(item['album']['release_date'],
                                                                 item['album']['release_date_precision']) for item in items]

            info__artists['artist_uri'] = [artist['id'] for item in items for artist in item['album']['artists']]
            info__artists['artist_name'] = [artist['name'] for item in items for artist in item['album']['artists']]

            info__ownerships['album_uri'] = info__albums['album_uri']
            info__ownerships['like_date'] = [item['added_at'] for item in items]

            info__albums['availability'] = [item['album']['available_markets'] for item in items]
            
            for i_0, i__0 in zip([info_albums, info_artists, info_ownerships], [info__albums, info__artists, info__ownerships]):
                for key in i__0.keys():
                    i_0[key] = i_0.get(key, []) + i__0[key]
                                                                        
            offset += len(items)
            self.show_progress(offset, total)

        albums_df = DataFrame(info_albums)
        artists_df = DataFrame(info_artists).drop_duplicates()
        ownerships_df = DataFrame(info_ownerships)

        return albums_df, artists_df, ownerships_df
    
    # # def get_albums_info(self):
    # #     return albums_df, artists_df, ownerships_df
    
    ''' extract saved playlists '''
    def get_playlists(self):
        albums_df, artists_df, ownerships_df = self.get_playlists_data()
        
        return albums_df, artists_df, ownerships_df
        
    def get_favorites(self):
        pass
        
        #return albums_df, artists_df, ownerships_df
        
    def get_playlists_data(self):
        self.announce('getting playlists data')
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
            albums_df, artists_df, ownerships_df = self.get_playlist_info(possible_lists)
                
        return albums_df, artists_df, ownerships_df
    
    def get_playlist_info(self, possible_lists):
        self.announce('analyzing playlists')
        info_albums = {}
        info_artists = {}
        info_ownerships = {}
        total_lists = len(possible_lists)
        for p, (p_list, count) in enumerate(possible_lists):
            self.show_progress(p, total_lists)
            info__albums = {}
            info__artists = {}
            info__ownerships = {}
                
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
                items = results['items']
                all_items.extend(items)

                offset += len(items)
                            
            if (count == 'single') and (len(all_items) > self.max_tracks_album_playlist):
                # this is not an acceptible configuration
                index_splits = None
                
            else:
                album_uris = [t['track']['album']['id'] for t in all_items]
                    
                match count:
                    case 'multiple':
                        # this is multiple albums on one playlist
                        index_splits = self.split_by_uri(album_uris)
                    
                    case 'single':
                        # this is one playlist
                        index_splits = [(0, len(album_uris))]
                
                for i, (start, end) in enumerate(index_splits):
                    items = all_items[start:end]
                    
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
                    info__albums['album_uri'] = [album_uri]
                    info__albums['album_name'] = [album_name]
                    info__albums['album_type'] = [album_type]
                    info__albums['upc'] = [upc]
                    info__albums['image_src'] = [image_src]
                                
                    info__albums['track_uris'] = [[t['track']['id'] for t in items]]
                    artist_uris = [[a['id'] for a in t['track']['artists']] for t in items]
                    info__albums['artist_uris'] =  [artist_uris] if len(artist_uris) <= self.max_artists_on_album else [[self.various_artist_uri]]
                    info__albums['album_duration'] = [sum(t['track']['duration_ms'] for t in items)/(1000*60)]
                    info__albums['release_date'] = [max(self.convert_release_date(t['track']['album']['release_date'],
                                                                             t['track']['album']['release_date_precision']) \
                                                                                    for t in items)]
                    
                    # artists
                    info__artists['artist_uri'] = [a['id'] for t in items for a in t['track']['artists']]
                    info__artists['artist_name'] = [a['name'] for t in items for a in t['track']['artists']]

                    # ownerships
                    info__ownerships['album_uri'] = [album_uri]
                    info__ownerships['like_date'] = [min(t['added_at'] for t in items)]
                
                    for i_0, i__0 in zip([info_albums, info_artists, info_ownerships], [info__albums, info__artists, info__ownerships]):
                        for key in i__0.keys():
                            i_0[key] = i_0.get(key, []) + i__0[key]
        
        self.show_progress(total_lists, total_lists)
        albums_df = DataFrame(info_albums)
        artists_df = DataFrame(info_artists).drop_duplicates()
        ownerships_df = DataFrame(info_ownerships)

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
        self.announce('getting track data')
        tracks_df = self.get_tracks_info(tracks_df['track_uri'].to_list())
        
        return tracks_df
    
    def get_tracks_info(self, track_uris):
        total_rows = len(track_uris)
        info_tracks = {}
        max_rows = ceil(total_rows/self.api_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            track_uris__0 = track_uris[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__tracks = {}
            
            results = self.sp.tracks(track_uris__0)
            self.sleep()
            
            info__tracks['track_uri'] = track_uris__0  
            info__tracks['track_name'] = [t['name'] for t in results['tracks']]
            info__tracks['artist_uris'] = [[a['id'] for a in t['artists']] for t in results['tracks']]
            info__tracks['isrc'] = [t['external_ids']['isrc'] for t in results['tracks']]
            info__tracks['track_duration'] = [t['duration_ms']/(1000*60) for t in results['tracks']]
            info__tracks['explicit'] = [t['explicit'] for t in results['tracks']]
                        
            for key in info__tracks.keys():
                info_tracks[key] = info_tracks.get(key, []) + info__tracks[key]

        self.show_progress(max_rows, max_rows)
        tracks_df = DataFrame(info_tracks)
        
        return tracks_df

    def get_soundtracks_data(self, tracks_df):
        self.announce('getting soundtrack data')
        tracks_df = self.get_soundtrack_info(tracks_df['track_uri'].to_list())
        return tracks_df
    
    def get_soundtrack_info(self, track_uris):
        total_rows = len(track_uris)
        info_tracks = {}
        max_rows = ceil(total_rows/self.api_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            track_uris__0 = track_uris[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__tracks = {}
            
            results = self.sp.audio_features(track_uris__0)
            self.sleep()
            
            info__tracks['track_uri'] = track_uris__0
            info__tracks['instrumentalness'] = [t['instrumentalness'] if t else 0 for t in results]
            
            for key in info__tracks.keys():
                info_tracks[key] = info_tracks.get(key, []) + info__tracks[key]

        self.show_progress(max_rows, max_rows)
        tracks_df = DataFrame(info_tracks)
        
        return tracks_df
    
    def get_artists_data(self, artists_df):
        self.announce('getting artists data')
        tracks_df = self.get_artists_info(artists_df['artist_uri'].to_list())
        
        return tracks_df
    
    def get_artists_info(self, artist_uris):
        total_rows = len(artist_uris)
        info_artists = {}
        max_rows = ceil(total_rows/self.api_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            artist_uris__0 = artist_uris[i*self.api_limit:min((i+1)*self.api_limit, total_rows)]
            info__artists = {}
            
            results = self.sp.artists(artist_uris__0)
            self.sleep()
            
            info__artists['artist_uri'] = artist_uris__0
            info__artists['artist_name'] = [a['name'] for a in results['artists']]
            info__artists['genres'] = [a['genres'] for a in results['artists']]
            
            for key in info__artists.keys():
                info_artists[key] = info_artists.get(key, []) + info__artists[key]

        self.show_progress(max_rows, max_rows)
        artists_df = DataFrame(info_artists)
        
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
    play_url = SOUNDCLOUD_PLAY_URL
    widget_url = SOUNDCLOUD_WIDGET_URL
    
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
        chrome_options.add_argument('--mute-audio')
        self.driver = webdriver.Chrome(options=chrome_options)

    def disconnect(self):
        self.driver.quit()
        
    def get_albums(self):
        self.announce('getting album data')
        albums_df, artists_df, ownerships_df = self.get_releases('albums')
        return albums_df, artists_df, ownerships_df

    def get_favorites(self):
        self.announce('getting favorites data')
        albums_df, artists_df, ownerships_df = self.get_releases('likes')
        return albums_df, artists_df, ownerships_df

    def get_releases(self, page):
        info_albums = {}
        info_artists = {}
        info_ownerships = {}
        
        page_length = 0
        self.driver.get(f'{self.play_url}/{self.username}/{page}')
        elem = self.find_element(self.driver, By.TAG_NAME, 'body')
            
        # fully load page
        while len(self.driver.page_source) > page_length:
            page_length = len(self.driver.page_source)
            elem.send_keys(Keys.PAGE_DOWN)
            sleep(0.2)
        
        playlist_elements = self.find_elements(self.driver, By.CLASS_NAME, 'soundList__item')
        if playlist_elements:
            total_playlists = len(playlist_elements)
            album_urls = []
            for i, playlist_element in enumerate(playlist_elements):
                self.show_progress(i, total_playlists)
                release_element = self.find_element(playlist_element, By.CLASS_NAME, 'sound__trackList')
                    
                # check that it could be a playlist
                if release_element:
                    title_element = self.find_element(playlist_element, By.CLASS_NAME, 'soundTitle__title')
                        
                    # check that there are tracks
                    if self.find_element(playlist_element, By.CLASS_NAME, 'compactTrackList__item'):
                        info__albums = {}
                        info__artists = {}
                        info__ownerships = {}
                        
                        album_url = title_element.get_attribute('href')
                        album_urls.append(album_url)
                        item = self.get_oembed(album_url)
                        artist_uri = self.extract_uri_from_url(item['author_url'])
                        artist_name = item['author_name']
                        date_element = self.find_element(playlist_element, By.CLASS_NAME, 'releaseDateCompact')

                        if date_element:
                            album_type = 'album'
                        else:
                            album_type = 'playlist'
                        
                        info__albums['artist_uris'] = [[artist_uri]]
                        info__albums['album_uri'] = [self.extract_uri_from_html(item['html'], 'playlist')]
                        info__albums['album_name'] = [self.extract_name_from_title(item['title'], artist_name)[0]]
                        info__albums['image_src'] = [item['thumbnail_url']]
                        info__albums['album_type'] = [album_type]
                        ##info__albums['upc'] = [None]

                        info__artists['artist_uri'] = [artist_uri]
                        info__artists['artist_name'] = [artist_name]

                        info__ownerships['album_uri'] = info__albums['album_uri']
                        info__ownerships['like_date'] = [datetime.today().date()]
                        
                        for i_0, i__0 in zip([info_albums, info_artists, info_ownerships], [info__albums, info__artists, info__ownerships]):
                            for key in i__0.keys():
                                i_0[key] = i_0.get(key, []) + i__0[key]
            self.show_progress(total_playlists, total_playlists)
                
            existing_keys = info__albums.keys()
            for i, album_url in enumerate(album_urls):
                self.show_progress(i, total_playlists)
                info__albums = {}
                info__tracks = self.get_tracklist(album_url)
                for key in info__tracks.keys():
                    if key in existing_keys:
                        info_albums[key][i] = info__tracks[key]
                    else:
                        info_albums[key] = info_albums.get(key, []) + [info__tracks[key]]
            self.show_progress(total_playlists, total_playlists)
                                
        albums_df = DataFrame(info_albums)
        artists_df = DataFrame(info_artists).drop_duplicates()
        ownerships_df = DataFrame(info_ownerships)
        
        return albums_df, artists_df, ownerships_df

    def get_tracklist(self, album_url):
        self.announce('analyzing albums')
        self.driver.get(album_url)
        sleep(2)
                    
        # check if there are more tracks
        more_element = self.find_element(self.driver, By.CLASS_NAME, 'compactTrackList__moreLink')
        if more_element:
            more_element.click()
            
        title_elements = self.find_elements(self.driver, By.CLASS_NAME, 'trackItem__trackTitle')
        track_uris = []
        for title_element in title_elements:
            track_url = title_element.get_attribute('href')
            item = self.get_oembed(track_url)
            track_uris.append(self.extract_uri_from_html(item['html'], 'track'))
       
        # get album duration
        duration_element = self.find_element(self.driver, By.CLASS_NAME, 'genericTrackCount__duration') # MM:SS
        if duration_element.text:
            minutes, seconds = duration_element.text.split(':')
            album_duration = int(minutes) + int(seconds)/60 
        else:
            album_duration = None
        
        # look for the release date
        time_element = self.find_element(self.driver, By.CLASS_NAME, 'relativeTime')
        listen_element = self.find_element(self.driver, By.CLASS_NAME, 'listenInfo__releaseData') # DD mmm YYY
        if time_element:
            r_date = time_element.get_attribute('title') # or datetime="2019-03-04T20:03:55.000Z"
        elif listen_element:
            r_date = listen_element.text
        release_date = datetime.strptime(r_date,  '%d %B %Y').date()
        
        # update album type to soundtrack if it's in the tag
        tag_element = self.find_element(self.driver, By.CLASS_NAME, 'sc-tag')
        album_type = None
        if tag_element:
            if 'soundtrack' in tag_element.text.lower():
                album_type = 'soundtrack'
        
        info__tracks = {}
        info__tracks['track_uris'] = track_uris
        info__tracks['album_duration'] = album_duration
        info__tracks['release_date'] = release_date
        if album_type:
            info__tracks['album_type'] = album_type

        return info__tracks        

    def get_tracks_data(self, tracks_df):
        self.announce('getting track data')
        
        info_tracks = {}
        max_rows = len(tracks_df)
        for i, track_uri in enumerate(tracks_df['track_uri']):
            self.show_progress(i, max_rows)
            info__tracks = {}
            # use widget to get the url
            self.driver.get(f'{self.widget_url}/tracks/{track_uri}')
            sleep(1)
            button_element = self.find_element(self.driver, By.CLASS_NAME, 'soundHeader__shareButton')
            button_element.click()
            link_code_input = self.find_element(self.driver, By.CLASS_NAME, 'sharePanel__linkCodeInput')
            
            if link_code_input:
                # get trackname and artist info from oembed
                link_code = link_code_input.get_attribute('value')
                url = link_code[:link_code.index('?utm_source')]
                item = self.get_oembed(url)
                artist_name = item['author_name']
                track_name, true_artist_name = self.extract_name_from_title(item['title'], artist_name)
                
                if artist_name == true_artist_name:
                    artist_uri = self.extract_uri_from_url(item['author_url'])
                else:
                    artist_uri = self.get_artist_uri(true_artist_name)

                # get track duration from webpage
                self.driver.get(link_code[:link_code.index('?utm_source')])
                sleep(1)
                timeline_element = self.find_element(self.driver, By.CLASS_NAME, 'playbackTimeline__duration')
                span_element = timeline_element.find_element(By.XPATH, './/span[@aria-hidden="true"]')
                minutes, seconds = span_element.text.split(':')
                duration = int(minutes) + int(seconds)/60

                info__tracks['track_uri'] = [track_uri]
                info__tracks['track_name'] = [track_name]
                info__tracks['artist_uris'] = [[artist_uri]]
                info__tracks['track_duration'] = [duration]
                
                for key in info__tracks.keys():
                    info_tracks[key] = info_tracks.get(key, []) + info__tracks[key]

        self.show_progress(max_rows, max_rows)
        tracks_df = DataFrame(info_tracks)
        
        return tracks_df
    
    def get_artist_uri(self, artist_name):
        self.driver.get(f'{self.play_url}/search/people?q={parse.quote(artist_name)}')
        sleep(1)

        # search through results for verified or most popular
        verified = False
        follows = []
        people_items = self.find_elements(self.driver, By.CLASS_NAME, 'searchList__item')
        for people_item in people_items:
            if self.find_element(people_item, By.CLASS_NAME, 'verifiedBadge'):
                verified = True
                break
            mini_stats_element = self.find_element(people_item, By.CLASS_NAME, 'sc-ministats-item')
            follower_count = int(mini_stats_element.get_attribute('title')[:-len(' followers')].replace(',', ''))
            follows.append(follower_count)
        if not verified:
            people_item = people_items[follows.index(max(follows))]
        
        link_element = self.find_element(people_item, By.CLASS_NAME, 'sc-link-primary')
        artist_uri = link_element.get_attribute('href')[:-len(self.play_url + ' ')]

        return artist_uri
        
    def get_artist_data(self, artists_df):
        self.announce('getting artist data')
        
        info_artists = {}
        max_rows = len(artists_df)
        for i, artist_uri in enumerate(artists_df['artist_uris']):
            info__artists = {}
            self.show_progress(i, max_rows)

            url = f'{self.play_url}/{artist_uri}'
            item = self.get_oembed(url)

            info__artists['artist_uri'] = [artist_uri]
            info__artists['artist_name'] = [item['author_name']]

            for key in info__artists.keys():
                info_artists[key] = info_artists.get(key, []) + info__artists[key]

        self.show_progress(max_rows, max_rows)
        artists_df = DataFrame(info_artists)
        
        return artists_df
                        
    def find_element(self, parent, by, name):
        return self.check_elements(parent, by, name, multiple=False)

    def find_elements(self, parent, by, name):
        return self.check_elements(parent, by, name, multiple=True)
    
    def check_elements(self, parent, by, name, multiple):
        try:
            if multiple:
                elements = parent.find_elements(by, name)
            else:
                elements = parent.find_element(by, name)
        except NoSuchElementException:
            elements = None
        return elements

    def get_oembed(self, url):
        response = requests.get(url=f'{self.play_url}/oembed', params={'url': url, 'format': 'json'})
        if response.ok:
            item = response.json()
        else:
            item = {}
        return item
        
    def extract_uri_from_html(self, html, uri_type):
        uri = html[html.index(f'{uri_type}s%2F')+len(f'{uri_type}s%2F'):html.index('&show_artwork')]
        return uri

    def extract_uri_from_url(self, url):
        uri = url[url.rindex('/')+1:]
        return uri
        
    def extract_name_from_title(self, title, artist_name):
        # remove SoundCloud add-on
        name = title[:-len(' by ' + artist_name)]
        
        # remove artist name
        artist_patterns = [rf'^{re.escape(artist_name)} - (.+)$',  # matches 'artist - song title'
                           rf'^{re.escape(artist_name)}\s+(.+)$',  # matches 'artist  song title'
                           r'^(.+?)\s{3}(.+)$',                    # matches 'other artist   song title'
                           r'^(.+)$',                              # matches 'song title'
                           ]
        number_patterns = [rf'^{re.escape(artist_name)} - (.+)$',  # matches 'artist - song title'
                           rf'^{re.escape(artist_name)}\s+(.+)$',  # matches 'artist  song title'
                           r'^(.+?)\s{3}(.+)$',                    # matches 'other artist   song title'
                           r'^(.+)$',                              # matches 'song title'
                           ]

        for pattern in artist_patterns:
            match = re.match(pattern, name)
            if match:
                if pattern == artist_patterns[2]:
                    name = match.group(2)
                    artist_name = match.group(1)  # return song title and true artist
                else:
                    name = match.group(1) # return song title and original artist

        # remove leading track number -> this will overcorrect if it's not actually a track number
        match = re.match(r'^(\d{1,2})\s+(.*)', name)
        if match:
            number = int(match.group(1))
            if 0 <= number < 30: # assume albums have less than 30 songs
                name = match.group(2)
        
        return name, artist_name
    
    def get_soundtracks_data(self, tracks_df):
        return tracks_df
        

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
           
    def get_billboard_albums(self, start_date, end_date, limit=100):
        restrictions = start_date and end_date
        i_range = range((datetime.today() - self.chart_start).days // 7 + 1)
        chart_range = [(self.chart_start + timedelta(days=i*7)) for i in i_range[::-1]]
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