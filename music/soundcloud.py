''' Streaming music sources and libraries '''

from datetime import datetime
import re
from collections import Counter

import requests

from common.secret import get_secret
from common.structure import SOUNDCLOUD_PLAY_URL, SOUNDCLOUD_SEARCH_URL, SOUNDCLOUD_RATE_LIMIT, SOUNDCLOUD_QUERY_LIMIT
from common.calling import KeyFinder
from music.dsp import DSP

class Sounder(DSP):
    name = 'SoundCloud'
    
    play_url = SOUNDCLOUD_PLAY_URL
    search_url = SOUNDCLOUD_SEARCH_URL
    api_rate_limit = SOUNDCLOUD_RATE_LIMIT
    api_query_limit = SOUNDCLOUD_QUERY_LIMIT
            
    def __init__(self):
        super().__init__()
        self.user_id = None
      
    def connect(self, user_id=None):
        self.client_id = KeyFinder.find_client_id(self.play_url)
        self.user_id = user_id

    def get_headers(self):
        return KeyFinder.mock_browser_headers(self.play_url)
    
    def get_params(self, params={}):
        params.update({'client_id': self.client_id})
        return params
        
    def get_offset(self, next_href):
        # Regular expression to capture the text between 'offset=' and '&'
        pattern = re.compile(r'offset=([^&]+)')
        match = re.search(pattern, next_href)
        if match:
            offset = match.group(1) if match else None
        else:
            offset = None
        return offset
        
    def get_albums(self):
        self.announce('getting albums')
        
        response = requests.get(f'{self.search_url}/users/{self.user_id}', params=self.get_params(),
                                    headers=self.get_headers())
        self.sleep()
        if response.ok:
            total_playlists = response.json().get('playlist_count', 0)
            
            info_albums = {}
            info_artists = {}
            info_ownerships = {}
                
            url = f'{self.search_url}/users/{self.user_id}/playlists'
            params = self.get_params({'limit': self.api_query_limit})
            next_href = True
            found_playlists = []
            info__albums = {}
            while next_href and len(found_playlists) < total_playlists:
                self.show_progress(len(found_playlists), total_playlists)
                response = requests.get(url, params=params, headers=self.get_headers())
                self.sleep()
                if not response.ok:
                    break
            
                items = response.json()
                next_href = items['next_href']
                if next_href:
                    params['offset'] = self.get_offset(items['next_href'])
            
                collection = items['collection']
                if len(collection):
                    playlists = [c for c in collection if c['is_album'] and c['track_count']] ## > min_track_count
                    found_playlists.append(collection)

                    album_names = [p['title'] for p in playlists]
                    self.add_text(' | '.join(album_names))
                
                    like_dates = [p['created_at'] for p in playlists]
                    image_srcs = [self.find_playlist_artwork(p) for p in playlists]
                    album_uris = [p['id'] for p in playlists]
                    r_dates = [p['release_date'] if p.get('release_date') else p['display_date'] for p in playlists]
                    release_dates = [datetime.strptime(r, '%Y-%m-%dT%H:%M:%SZ') for r in r_dates]
                    release_types = [p['set_type'] for p in playlists]
                    track_uris = [[t['id'] for t in p['tracks']] for p in playlists]
                    barcodes = [False] * len(playlists)

                    artist_names = []
                    artist_uris = []
                    for playlist in playlists:
                        a_names, a_uris = self.find_true_playlist_artist(playlist)
                        artist_names.append(a_names)
                        artist_uris.append(a_uris)
                    
                    durations = [p['duration']/(1000*60) for p in playlists]
            
                    info__albums['album_uri'] = album_uris
                    info__albums['album_name'] = album_names
                    info__albums['album_type'] = release_types
                    info__albums['release_date'] = release_dates
                    info__albums['image_src'] = image_srcs
                    info__albums['artist_uris'] = artist_uris
                    info__albums['track_uris'] = track_uris
                    info__albums['upc'] = barcodes
                    info__albums['album_duration'] = durations
                
                    info__artists = {}
                    info__artists['artist_uri'] = artist_uris
                    info__artists['artist_name'] = artist_names
                
                    info__ownerships = {}
                    info__ownerships['album_uri'] = album_uris
                    info__ownerships['like_date'] = like_dates

                    info_albums, info_artists, info_ownerships = \
                        self.combine_infos([info_albums, info_artists, info_ownerships],
                                           [info__albums, info__artists, info__ownerships])
        
            self.show_progress()
        
            albums_df = self.get_df_from_info(info_albums, subset=['album_uri'])
            artists_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
            ownerships_df = self.get_df_from_info(info_ownerships, subset=['album_uri'])

            return albums_df, artists_df, ownerships_df
    
    def get_favorites(self):
        self.announce('getting favorites')
    
        response = requests.get(f'{self.search_url}/users/{self.user_id}', params=self.get_params(), headers=self.get_headers())
        self.sleep()
        if response.ok:
            total_playlists = response.json().get('playlist_likes_count', 0)
            
        info_albums = {}
        info_ownerships = {}
                
        url = f'{self.search_url}/users/{self.user_id}/likes'
        self.sleep()
        
        params = self.get_params({'limit': self.api_query_limit})

        next_href = True
        found_playlists = []
        while next_href and len(found_playlists) < total_playlists:
            response = requests.get(url, params=params, headers=self.get_headers())
            self.sleep()
            if not response.ok:
                break
            
            items = response.json()
            next_href = items['next_href']
            if next_href:
                params['offset'] = self.get_offset(items['next_href'])
            
            collection = items['collection']
            if len(collection):
                found_playlist_likes = [c for c in collection if c.get('playlist') \
                                  and c['playlist']['id'] not in found_playlists] # in case repeats with offset
                found_playlists.extend(f['playlist']['id'] for f in found_playlist_likes)
                
                playlist_likes = [c for c in found_playlist_likes if c['playlist']['track_count']]
                playlists = [l['playlist'] for l in playlist_likes]
                
                album_names = [p['title'] for p in playlists]
                self.add_text(' | '.join(album_names))

                like_dates = [l['created_at'] for l in playlist_likes]
                
                album_uris = [p['id'] for p in playlists]

                r_dates = [p['release_date'] if p.get('release_date') else p['display_date'] for p in playlists]
                release_dates = [datetime.strptime(r, '%Y-%m-%dT%H:%M:%SZ') for r in r_dates]
                release_types = [(p['set_type'] if p['is_album'] else p['kind']) for p in playlists]
                artist_uris = [[]]
                artist_names = [[]]
                durations = [p['duration']/(1000*60) for p in playlists]
                barcodes = [False] * len(playlists)
            

                info__albums = {}
                info__albums['album_uri'] = album_uris
                info__albums['album_name'] = album_names
                info__albums['album_type'] = release_types
                info__albums['release_date'] = release_dates
                info__albums['image_src'] = [] # fill in from tracks
                info__albums['artist_uris'] = [] # fill in from tracks
                info__albums['track_uris'] = [] # fill in from tracks
                info__albums['upc'] = barcodes
                info__albums['album_duration'] = durations
                
                info__ownerships = {}
                info__ownerships['album_uri'] = album_uris
                info__ownerships['like_date'] = like_dates

                info_albums, info_ownerships = \
                    self.combine_infos([info_albums, info_ownerships],
                                       [info__albums, info__ownerships])
        
        self.show_progress()
        
        self.announce('analyzing favorites')
        
        info_artists = {}
        for i, album_uri in enumerate(info_albums['album_uri']):
            self.show_progress(i, total_playlists)
            track_uris, artist_names, artist_uris, image_src = self.get_playlist_details(album_uri)
            
            info_albums['track_uris'].append(track_uris)
            info_albums['artist_uris'].append(artist_uris)
            info_albums['image_src'].append(image_src)
            
            info__artists = {}
            info__artists['artist_uri'] = artist_uris
            info__artists['artist_name'] = artist_names

            info_artists = self.combine_infos(info_artists, info__artists)
            
        self.show_progress()
        
        albums_df = self.get_df_from_info(info_albums, subset=['album_uri'])
        artists_df = self.get_df_from_info(info_artists, subset=['artist_uri'])
        ownerships_df = self.get_df_from_info(info_ownerships, subset=['album_uri'])
                
        return albums_df, artists_df, ownerships_df

    def get_playlist_details(self, playlist_uri):
        url = f'{self.search_url}/playlists/{playlist_uri}'
        response = requests.get(url, params=self.get_params(), headers=self.get_headers())
        self.sleep()
        if response.ok:
            playlist = response.json()
            self.add_text(playlist.get('title'))
            genre = playlist['genre']
            tags = playlist['tag_list']
            tracks = playlist['tracks']
            track_uris = [t['id'] for t in tracks]
            image_src = self.find_playlist_artwork(playlist)

            artist_names, artist_uris = self.find_true_playlist_artist(playlist)
            
            return track_uris, artist_names, artist_uris, image_src
            
    def get_tracks_data(self, tracks_df):
        self.announce('getting tracks data')
        
        info_tracks = {}
        total_rows = len(tracks_df)
        for i, track_uri in enumerate(tracks_df['track_uri']):
            self.show_progress(i, total_rows)
            info__tracks = self.get_track_info(track_uri)
            info_tracks = self.combine_infos(info_tracks, info__tracks)
        
        self.show_progress()
        tracks_df = self.get_df_from_info(info_tracks)
        
        return tracks_df

    def get_track_info(self, track_uri):
        url = f'{self.search_url}/tracks/{track_uri}'
        response = requests.get(url, params=self.get_params(), headers=self.get_headers())
        self.sleep()
        if response.ok:
            track = response.json()
            user_name = self.get_user_artist_name(track['user'])
            self.add_text(f'{user_name} -  {track["title"]}')
            title, artist_name = self.remove_artist_name_from_title(track['title'], user_name)
            self.add_text(f'{artist_name} - {title}')

            # find the artist
            if (publisher:=track.get('publisher_metadata')) and (publisher_artist_name:=publisher.get('artist')):
                artist_uri = self.find_true_artist(publisher_artist_name)['id']
                
            elif artist_name != track['user']['full_name']:
                # artist name is within the track title
                artist = self.find_true_artist(artist_name)
                artist_name = self.get_user_artist_name(artist)
                artist_uri = artist['id']
                
            else:
                # artist name is the username
                artist_uri = track['user']['id']
            self.add_text(f'{artist_name} - {title}')
            
            info__tracks = {}
            info__tracks['track_uri'] = [track_uri]
            info__tracks['track_name'] = [title]
            info__tracks['track_duration'] = [track['duration']/(1000*60)]
            info__tracks['artist_uris'] = [[artist_uri]]
            
            return info__tracks

    def get_artist_data(self, artists_df):
        self.announce('getting tracks data')
        
        info_artists = {}
        total_rows = len(artists_df)
        for i, artist_uri in enumerate(artists_df['artist_uri']):
            self.show_progress(i, total_rows)
            info__artists = self.get_artist_info(artist_uri)
            info_artists = self.combine_infos(info_artists, info__artists)
        
        self.show_progress()
        artists_df = self.get_df_from_info(info_artists)
        
        return artists_df
    
    def get_artist_info(self, artist_uri):
        url = f'{self.search_url}/users/{artist_uri}'
        response = requests.get(url, params=self.get_params(), headers=self.get_headers())
        self.sleep()
        if response.ok:
            artist = response.json()
            artist_name = self.get_user_artist_name(artist)
            self.add_text(artist_name)
            
            info__artist = {}
            info__artist['artist_uri'] = [artist_uri]
            info__artist['artist_name'] = [artist_name]
            
            return info__artist
    
    def sort_artist_key(self, artist):
        badges = artist['badges']
        return (not badges['verified'],  # false comes before True, so we negate it
                not badges['pro_unlimited'],
                not badges['pro'],
                -artist['followers_count']  # negative for descending order
                )

    def get_user_artist_name(self, user):
        artist_name = user['full_name'] if user['full_name'] else user['username']
        return artist_name

    def find_true_artist(self, artist_name):
        url = f'{self.search_url}/search/users?q={artist_name}'
        response = requests.get(url, params=self.get_params(), headers=self.get_headers())
        self.sleep()
        if response.ok:
            artists = response.json()['collection']
            if len(artists):
                true_artist = sorted(artists, key=lambda x: self.sort_artist_key(x))[0]
            else:
                true_artist = None
                
        return true_artist
    
    def get_publisher_name(self, track):
        return publisher_artist_name if (publisher:=track.get('publisher_metadata')) and (publisher_artist_name:=publisher.get('artist')) else None
        
    def find_true_playlist_artist(self, playlist):
        threshold = 0.5
        tracks = playlist['tracks']
        publisher_names = [publisher_name if (publisher_name:=self.get_publisher_name(track)) else None for track in tracks]
        title_names = [self.extract_name_from_title(track['title'], self.get_user_artist_name(track['user']))[-1] \
                       if track.get('title') else None for track in tracks]
        artist_names = [p if p else t if t else None for p, t in zip(publisher_names, title_names)]
        
        # at this point, we can build a dictionary
        unique_names = set(artist_names)
        true_artists = {name: self.find_true_artist(name) for name in unique_names if name}

        # map each track name to its true artist ID
        artist_uris = [true_artists[name]['id'] for name in artist_names if name and true_artists[name]]

        # count the occurrences of each artist URI
        uri_counter = Counter(artist_uris)

        # check if any artist uri occurs more than 50% of the time
        frequent_uris = [uri for uri, count in uri_counter.items() if count > threshold * len(artist_uris)] ## should be len(artist_names)
        
        if frequent_uris:
            frequent_users = [true_artist for uri in frequent_uris for _, true_artist in true_artists.items() \
                              if true_artist['id'] == uri]
            artist_names = [self.get_user_artist_name(user) for user in frequent_users]
            artist_uris = frequent_uris
            
        else:
            artist_names = [self.get_user_artist_name(playlist['user'])]
            artist_uris = [playlist['user']['id']]
            
        return artist_names, artist_uris

    def find_playlist_artwork(self, playlist):
        image_src = playlist.get('artwork_url')
        if not image_src:
            image_src = next((t.get('artwork_url') for t in playlist['tracks']), None)
        return image_src

    def remove_track_num_from_title(self, title, track_num):
        # regular expression pattern to match track number with optional leading zeros, spaces, or hyphens
        pattern = re.compile(r'^\s*0*{}\s*[-\s]*'.format(re.escape(str(track_num))))
        # substitute the matched pattern with an empty string
        return re.sub(pattern, '', title).strip()
    
    def remove_artist_name_from_title(self, title, artist_name):
        # remove artist name
        patterns = [rf'^{re.escape(artist_name)} - (.+)$',  # matches 'artist - song title'
                    rf'^{re.escape(artist_name)}\s+(.+)$',  # matches 'artist  song title'
                    r'^(.+?)\s{3}(.+)$',                    # matches 'other artist   song title'
                    r'^(.+)$',                              # matches 'song title'
                    ]

        for pattern in patterns:
            match = re.match(pattern, title)
            if match:
                if pattern == patterns[2]:
                    title = match.group(2)
                    artist_name = match.group(1)  # return song title and true artist
                else:
                    title = match.group(1) # return song title and original artist
                    
        return title, artist_name
        
    def extract_name_from_title(self, title, artist_name, track_num=None): ##, url=None):
        # remove track number if it's before the artist name
        if track_num:
            title = self.remove_track_num_from_title(title, track_num)

        # remove artist name if it appears
        title, artist_name = self.remove_artist_name_from_title(title, artist_name)
        
        # remove track number if it's after the artist name
        if track_num:
            title, artist_name = self.remove_track_num_from_title(title, track_num)
        
        return title, artist_name    

