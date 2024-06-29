''' Streaming music sources and libraries '''

from math import ceil
from datetime import datetime
from base64 import b64encode, urlsafe_b64encode
import itertools
import statistics
from collections import Counter

import six
from spotipy import Spotify, SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler

from common.secret import get_secret, get_token
from common.structure import (SPOTIFY_AUTH_URL, SPOTIFY_LOGIN_URL, SPOTIFY_TOKENS_FOLDER,
                              SPOTIFY_REDIRECT_URI, SPOTIFY_SCOPE, SPOTIFY_PLAYLIST_WORD,
                              SPOTIFY_RATE_LIMIT, SPOTIFY_QUERY_LIMIT)
from music.dsp import DSP

class Spotter(DSP):
    name = 'Spotify'
    
    auth_url = SPOTIFY_AUTH_URL
    login_url = SPOTIFY_LOGIN_URL
    api_rate_limit = SPOTIFY_RATE_LIMIT
    api_query_limit = SPOTIFY_QUERY_LIMIT
        
    vinyl_word = SPOTIFY_PLAYLIST_WORD

    def __init__(self):
        super().__init__()
        self.sp = None
        self.user_id = None
        
        self.scope = SPOTIFY_SCOPE
        
    def get_auth_header(self):
        auth_header = urlsafe_b64encode((get_secret('SPOTIFY_CLIENT_ID') + ':' + get_secret('SPOTIFY_CLIENT_SECRET')).encode())
        
        headers = {
                  'Content-Type': 'application/x-www-form-urlencoded',
                  'Authorization': f'Basic {auth_header}' #'.decode("ascii")}'
                  }
        return headers #auth_header
    
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
            
            total = results['total']
            items = results['items']
            
            info__albums = {}
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

            info__artists = {}
            info__artists['artist_uri'] = [artist['id'] for item in items for artist in item['album']['artists']]
            info__artists['artist_name'] = [artist['name'] for item in items for artist in item['album']['artists']]

            info__ownerships = {}
            info__ownerships['album_uri'] = info__albums['album_uri']
            info__ownerships['like_date'] = [item['added_at'] for item in items]

            info_albums, info_artists, info_ownerships = \
                self.combine_infos([info_albums, info_artists, info_ownerships],
                                   [info__albums, info__artists, info__ownerships])
                                                                        
            offset += len(items)
            self.show_progress(offset, total)

        albums_df = self.get_df_from_info(info_albums, subset=['album_uri'])
        artists_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
        ownerships_df = self.get_df_from_info(info_ownerships, subset=['album_uri'])

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
            p_image_src = results['images'][0]['url']
                
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
                
                    info_albums, info_artists, info_ownerships = \
                        self.combine_infos([info_albums, info_artists, info_ownerships],
                                           [info__albums, info__artists, info__ownerships])
        
        self.show_progress()
        albums_df = self.get_df_from_info(info_albums, subset=['album_uri'])
        artists_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
        ownerships_df = self.get_df_from_info(info_ownerships, subset=['album_uri'])

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
        max_rows = ceil(total_rows/self.api_query_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            track_uris__0 = track_uris[i*self.api_query_limit:min((i+1)*self.api_query_limit, total_rows)]
            info__tracks = {}
            
            results = self.sp.tracks(track_uris__0)
            self.sleep()
            
            info__tracks['track_uri'] = track_uris__0  
            info__tracks['track_name'] = [t['name'] for t in results['tracks']]
            info__tracks['artist_uris'] = [[a['id'] for a in t['artists']] for t in results['tracks']]
            info__tracks['isrc'] = [t['external_ids']['isrc'] for t in results['tracks']]
            info__tracks['track_duration'] = [t['duration_ms']/(1000*60) for t in results['tracks']]
            info__tracks['explicit'] = [t['explicit'] for t in results['tracks']]
                        
            info_tracks = self.combine_infos(info_tracks, info__tracks)

        self.show_progress()
        tracks_df = self.get_df_from_info(info_tracks, subset=['track_uri'])
        
        return tracks_df

    def get_soundtracks_data(self, tracks_df):
        self.announce('getting soundtrack data')
        tracks_df = self.get_soundtrack_info(tracks_df['track_uri'].to_list())
        return tracks_df
    
    def get_soundtrack_info(self, track_uris):
        total_rows = len(track_uris)
        info_tracks = {}
        max_rows = ceil(total_rows/self.api_query_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            track_uris__0 = track_uris[i*self.api_query_limit:min((i+1)*self.api_query_limit, total_rows)]
            info__tracks = {}
            
            results = self.sp.audio_features(track_uris__0)
            self.sleep()
            
            info__tracks['track_uri'] = track_uris__0
            info__tracks['instrumentalness'] = [t['instrumentalness'] if t else 0 for t in results]
            
            info_tracks = self.combine_infos(info_tracks, info__tracks)

        self.show_progress()
        tracks_df = self.get_df_from_info(info_tracks, subset=['track_uri'])
        
        return tracks_df
    
    def get_artists_data(self, artists_df):
        self.announce('getting artists data')
        tracks_df = self.get_artists_info(artists_df['artist_uri'].to_list())
        
        return tracks_df
    
    def get_artists_info(self, artist_uris):
        total_rows = len(artist_uris)
        info_artists = {}
        max_rows = ceil(total_rows/self.api_query_limit)
        for i in range(max_rows):
            self.show_progress(i, max_rows)
            artist_uris__0 = artist_uris[i*self.api_query_limit:min((i+1)*self.api_query_limit, total_rows)]
            info__artists = {}
            
            results = self.sp.artists(artist_uris__0)
            self.sleep()
            
            info__artists['artist_uri'] = artist_uris__0
            info__artists['artist_name'] = [a['name'] for a in results['artists']]
            info__artists['genres'] = [a['genres'] for a in results['artists']]
            
            info_artists = self.combine_infos(info_artists, info__artists)

        self.show_progress()
        artists_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
        
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
        return file_name