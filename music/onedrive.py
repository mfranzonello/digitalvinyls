''' Streaming music sources and libraries '''

from datetime import datetime
import statistics
import re
from urllib import parse

import requests

from common.secret import get_secret, get_token
from common.structure import (AZURE_LOGIN_URL, AZURE_GRAPH_URL, AZURE_TOKENS_FOLDER,
                              AZURE_SCOPE, AZURE_VINYLS_FOLDER, AZURE_RATE_LIMIT)
from music.dsp import DSP

class Driver(DSP):
    name = 'OneDrive'

    login_url = AZURE_LOGIN_URL
    graph_url = AZURE_GRAPH_URL
    api_rate_limit = AZURE_RATE_LIMIT
    skiptoken = '$skiptoken'
    
    vinyls_path = AZURE_VINYLS_FOLDER
    
    def __init__(self):
        super().__init__()
        self.user_id = None
        self.access_token = None
        
    def get_header(self):
        return {'Content-Type': 'application/x-www-form-urlencoded'}

    def get_auth_headers_form(self):
        return {'Content-type': 'application/x-www-form-urlencoded',
                'Authorization': f'Bearer {self.access_token}'}
    
    def get_auth_headers_json(self):
        return {'Content-type': 'application/json',
                'Authorization': f'Bearer {self.access_token}'}
        
    def connect(self, user_id=None, uri=None):
        if uri:
            user_id = self.extract_user_id_from_uri(uri)
            
        if user_id:
            self.user_id = user_id

            # get the refresh token for this user from the json file
            refresh_token = get_token(AZURE_TOKENS_FOLDER, user_id)['refresh_token']
           
            # get access token from a refresh token
            data = {'client_id': get_secret('AZURE_CLIENT_ID'),
                    ##'client_secret': get_secret('AZURE_CLIENT_SECRET'), # don't send for public
                    'scope': AZURE_SCOPE,
                    'grant_type': 'refresh_token',
                    'refresh_token': refresh_token}
            response = requests.post(f'{self.login_url}/token', data=data, headers=self.get_header())
            self.sleep()
        
            if response.ok:
                params = response.json()
                access_token = params['access_token']
                ##refresh_token = params['refresh_token'] # should replace old refresh token
                ##expires_in = params['expires_in']       # and expiration
                self.access_token = access_token

                self.set_drive_id()
                self.set_vinyls_folder_id()

            else:
                self.display_status(response)

    def get_query(self, resource, params={}):
        # for one single query
        response = requests.get(f'{self.graph_url}/{resource}', params=params, headers=self.get_auth_headers_form())
        self.sleep()
        if response.ok:
            return response
        else:
            self.display_status(response)
            return self.no_response()
            
    def get_values(self, resource, params={}):
        # for a query that has multiple values
        values = []
        more = True
        while more:
            response = requests.get(f'{self.graph_url}/{resource}', params=params, headers=self.get_auth_headers_form())
            self.sleep()
            if response.ok:
                values.extend(response.json()['value'])
                more = response.json().get('@odata.nextLink', False)
                if more:
                    params[self.skiptoken]= more[more.index(f'{self.skiptoken}=') + len(f'{self.skiptoken}='):]
            else:
                self.display_status(response)
                break
                
        return values

    def set_drive_id(self):
        response = self.get_query(f'users/{self.user_id}/drive')
        self.drive_id = response.json()['id'].lower()
        
    def get_folders(self, item_id=None):
        if item_id:
            folder_id = f'/items/{item_id}'
        else:
            folder_id = f'/root'
        
        folders = self.get_values(f'drives/{self.drive_id}/{folder_id}/children')

        return folders

    def create_folder(self, parent_id, folder_name):
        data = {'name': folder_name,
                'folder': {},
                '@microsoft.graph.conflictBehavior': 'fail'}
        response = requests.post(f'{self.graph_url}/drives/{self.drive_id}/items/{parent_id}/children',
                                 json=data, headers=self.get_auth_headers_json())
        self.sleep()
        if response.ok:
            return response.json()
        else:
            print(response.url)
            self.display_status(response)
            return self.no_response()
    
    def get_files(self, item_id, media_type=None, extensions=None):
        files = self.get_values(f'drives/{self.drive_id}/items/{item_id}/children')
        if media_type:
            files = [f for f in files if f.get(media_type)]
        elif extensions:
            files = [f for f in files if any(f['name'][-len(f'.{e}'):].lower() == f'.{e}'.lower() for e in extensions)]
        return files
    
    def get_item_from_uri(self, item_uri):
        response = self.get_query(f'drives/{self.drive_id}/items/{item_uri}')
        if response.ok:
            return response.json()
        
    def extract_user_id_from_uri(self, uri):
        user_id, _ = uri.split('!')
        return user_id

    def find_folder(self, folder_name):
        items = self.get_values(f"drives/{self.drive_id}/root/search(q='{folder_name}')")

        if items:
            folders = [f for f in items if f['name'].lower() == folder_name.lower() and f.get('folder')]
            
        else:
            folders = []
            
        return folders
    
    def set_vinyls_folder_id(self):
        self.set_drive_id()
        folder_name = self.vinyls_path.split('/')[-1]
        folders = self.find_folder(folder_name)
        vinyls_folders = [f for f in folders if \
                          f"/{f['parentReference']['path']}/{folder_name}"[-len(self.vinyls_path):].lower() == self.vinyls_path.lower()]
        if len(vinyls_folders):
            self.vinyls_folder_id = vinyls_folders[0]['id']

    def get_statistic(self, items, stat):
        reduced = [i for i in items if i is not None]
        if len(reduced):
            match stat:
                case 'mode':
                    return statistics.mode(reduced)
                case 'max':
                    return max(reduced)
                case 'min':
                    return min(reduced)
                     
    def get_albums(self):
        self.announce('getting albums')

        artist_folders = self.get_folders(self.vinyls_folder_id)
        
        info_albums = {}
        info_artists = {}
        info_ownerships = {}
        total_artists = len(artist_folders)
        for i, artist_folder in enumerate(artist_folders):
            self.show_progress(i, total_artists, message=artist_folder['name'])
        
            album_folders = self.get_folders(artist_folder['id'])
            total_albums = len(album_folders)
            for j, album_folder in enumerate(album_folders):
                self.show_progress(i + j/total_albums, total_artists, message=artist_folder['name'] + ' - ' + album_folder['name'])
                album_uri = album_folder['id']
                
                tracks = self.get_files(album_folder['id'], media_type='audio')
                
                if len(tracks):
                
                    track_album_names = [t['audio'].get('album') for t in tracks]
                    album_name = mode_album_name if (mode_album_name:=self.get_statistic(track_album_names, 'mode')) else album_folder['name']
                
                    track_album_artist = [t['audio'].get('albumArtist') for t in tracks]
                    artist_name = mode_album_artist if (mode_album_artist:=self.get_statistic(track_album_artist, 'max')) else artist_folder['name']
                    artist_uri = self.get_artist_uri(artist_name)
                     
                    track_durations = [t['audio']['duration']/(1000*60) for t in tracks]
                    track_years = [t['audio'].get('year') for t in tracks]
                    release_date = datetime(max_track_year, 1, 1).date() if (max_track_year:=self.get_statistic(track_years, 'max')) else None
                                
                    track_nums = [t['audio'].get('track') for t in tracks]
                    if all(n is None for n in track_nums):
                        track_nums = self.extract_track_nums_from_filenames(track['audio'].get('title', track['name']) for track in tracks)
                    track_uris = self.sort_by_track_number([track['id'] for track in tracks], track_nums)
                    
                    track_genres = [t['audio'].get('genre') for t in tracks]
                    genre = self.get_statistic(track_genres, 'mode')
                    album_duration = sum(track_durations)
                    if album_duration > self.min_album_duration and len(track_uris) > self.min_album_tracks:
                        if genre and genre.lower() in ['soundtrack', 'score']:
                            album_type = genre.lower()
                        else:
                            album_type = 'album'
                    elif len(track_uris) > self.min_ep_tracks:
                        album_type = 'ep'
                    else:
                        album_type = 'single'
                    
                    like_date = datetime.strptime(album_folder['createdDateTime'], '%Y-%m-%dT%H:%M:%S.%fZ').date()
                
                    images = self.get_files(album_folder['id'], media_type='image')
                    image_src = images[0].get('@microsoft.graph.downloadUrl') if len(images) else None
                
                    info__albums = {}
                    info__albums['album_uri'] = [album_uri]
                    info__albums['album_name'] = [album_name]
                    info__albums['album_type'] = [album_type]
                    info__albums['release_date'] = [release_date]
                    info__albums['image_src'] = [image_src]
                    info__albums['artist_uris'] = [[artist_uri]]
                    info__albums['track_uris'] = [track_uris]
                    info__albums['upc'] = [False]
                    info__albums['album_duration'] = [album_duration]
                
                    info__artists = {}
                    info__artists['artist_uri'] = [artist_uri]
                    info__artists['artist_name'] = [artist_name]
                
                    info__ownerships = {}
                    info__ownerships['album_uri'] = [album_uri]
                    info__ownerships['like_date'] = [like_date]
                    
                    info_albums, info_artists, info_ownerships = \
                        self.combine_infos([info_albums, info_artists, info_ownerships],
                                           [info__albums, info__artists, info__ownerships])
                
        self.show_progress()
        albums_df = self.get_df_from_info(info_albums, subset=['album_uri'])
        artists_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
        ownerships_df = self.get_df_from_info(info_ownerships, subset=['album_uri'])

        return albums_df, artists_df, ownerships_df
    
    def get_tracks_data(self, tracks_df):
        self.announce('getting tracks data')
        if not self.user_id:
            self.connect(uri = tracks_df['track_uri'].iloc[0])
        
        info_tracks = {}
        total_tracks = len(tracks_df)
        for i, track_uri in enumerate(tracks_df['track_uri']):
            self.show_progress(i, total_tracks, message=track_uri)
            
            track = self.get_item_from_uri(track_uri)
            track_name = track['audio'].get('title', self.extract_track_name_from_filename(track['name']))
            artist_uris = [self.get_artist_uri(artist_name) for artist_name in track['audio'].get('artist', self.extract_artist_name_from_filepath(track['parentReference']['path'])).split(';')]
            track_duration = track['audio']['duration']/(1000*60)
            
            info__tracks = {}
            info__tracks['track_uri'] = [track_uri]
            info__tracks['track_name'] = [track_name]
            info__tracks['artist_uris'] = [artist_uris]
            info__tracks['isrc'] = [False]
            info__tracks['track_duration'] = [track_duration]
            #info__tracks['explicit']

            info_tracks = self.combine_infos(info_tracks,
                                             info__tracks)

        self.show_progress()        
        tracks_df = self.get_df_from_info(info_tracks, subset=['track_uri'])
        
        return tracks_df
        
    def get_artists_data(self, artists_df):
        self.announce('getting artists data')
        if not self.user_id:
            self.connect(uri = artists_df['artist_uri'].iloc[0])
        
        info_artists = {}
        total_artists = len(artists_df)
        for i, artist_uri in enumerate(artists_df['artist_uri']):
            self.show_progress(i, total_artists, message=artist_uri)
            
            artist = self.get_item_from_uri(artist_uri)
            artist_name = artist['name']

            info__artists = {}
            info__artists['artist_uri'] = [artist_uri]
            info__artists['artist_name'] = [artist_name]

            info_artists = self.combine_infos(info_artists,
                                              info__artists)

        self.show_progress()        
        artist_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
        
        return artist_df
        
    def get_soundtracks_data(self, tracks_df):
        if not self.user_id:
            self.connect(uri = tracks_df['track_uri'].iloc[0])
        
        # get the track download file
        # run it through an ffmpeg analyzer
        # get the instrumentalness
        return tracks_df
    
    def extract_number_from_filename(self, filename):
        match = re.match(r'^\s*(\d+)', filename)
        if match:
            return int(match.group(1))
        return None

    def extract_track_nums_from_filenames(self, filenames):
        track_nums = [self.extract_number(filename) for filename in filenames]
    
        # Check if there's only one non-None number in track_nums
        non_none_numbers = [num for num in track_nums if num is not None]
        if len(non_none_numbers) == 1:
            track_nums = [None] * len(filenames)
    
        return track_nums
    
    def extract_track_name_from_filename(self, filename):
        pattern = r'^\d*\s*-?\s*(.*?)\.[^.]*$'
        match = re.match(pattern, filename)
        track_name = match.group(1) if match else filename
        return track_name
    
    def extract_artist_name_from_filepath(self, filepath):
        artist_name = parse.unquote(filepath.split('/')[-2]) # '../{artist_name}/{album_name}'
        return artist_name

    def sort_by_track_number(self, track_uris, track_nums):
        if all(num is None for num in track_nums):
            # no track numbers, list already sorted
            sorted_uris = track_uris
            
        else:
            # Combine the two lists into a list of tuples
            tracks = list(zip(track_uris, track_nums))
    
            # Separate tracks with numbers and without numbers
            with_nums = sorted([(uri, num) for uri, num in tracks if num is not None], key=lambda x: x[1])
            without_nums = [(uri, num) for uri, num in tracks if num is None]
    
            # Merge them back together, filling in gaps with None tracks
            sorted_tracks = []
            num_set = {num for _, num in with_nums}
            expected_num = 1
    
            while with_nums or without_nums:
                if expected_num in num_set:
                    sorted_tracks.append(with_nums.pop(0))
                else:
                    if without_nums:
                        sorted_tracks.append(without_nums.pop(0))
                    else:
                        break
                expected_num += 1

            # If any remaining without_nums, add them at the end
            sorted_tracks.extend(without_nums)
    
            # Extract just the URIs for the final result
            sorted_uris = [uri for uri, _ in sorted_tracks]
    
        return sorted_uris

    def check_artist_name(self, artist_name):
        artist_folders = self.get_folders(self.vinyls_folder_id)
        artist_uris = [f['id'] for f in artist_folders]
        artist_names = [f['name'] for f in artist_folders]
        if artist_name in artist_names:
            artist_uri = artist_uris[[n.lower() for n in artist_names].index(artist_name.lower())]
        else:
            artist_uri = None
        return artist_uri
    
    def add_artist_uri(self, artist_name):
        artist_folder = self.create_folder(self.vinyls_folder_id, artist_name)
        artist_uri = artist_folder['id']
        return artist_uri
    
    def get_artist_uri(self, artist_name):
        artist_uri = self.check_artist_name(artist_name)
        if not artist_uri:
            artist_uri = self.add_artist_uri(artist_name)
        return artist_uri
        