''' Sonos API functions - requires dev keys + user auth / local network access '''

from datetime import datetime
#from base64 import b64encode
from urllib import parse

import requests
from requests.auth import HTTPBasicAuth
#from soco.data_structures import DidlItem, to_didl_string
from soco.discovery import by_name as discover_by_name
from soco.music_services import MusicService

from ..common.calling import Caller
from ..common.structure import SONOS_AUTH_URL, SONOS_CONTROL_URL, SONOS_TOKENS_FOLDER, SONOS_REDIRECT_URI
                             #SONOS_HOST, SONOS_PORT)
from ..common.secret import get_secret, get_token, save_token

class Sonoser(Caller):
    login_url = SONOS_AUTH_URL
    control_url = SONOS_CONTROL_URL
    
    callouts = {'Spotify': 'spotify:spotify%3atrack%3a',
                'SoundCloud': 'http:track-%3esoundcloud%3atracks%3a',
                'OneDrive': 'http:',
                'YouTube Music': 'hls-static',
                }

    def __init__(self):
        super().__init__()
        self.access_token = None
        
        self.household_id = None
        self.owner_id = None
        self.groups = {}
        self.controller_id = None
        self.players = {}
        self.controller_name = None
        #self.session_id = None
        
    def get_auth_header(self):
        auth_header = HTTPBasicAuth(get_secret('SONOS_CLIENT_KEY'), get_secret('SONOS_CLIENT_SECRET'))
        return auth_header

    # # def get_access(self, user_email):
    # #     code = get_token(SONOS_TOKENS_FOLDER, user_email)['code']
    # #     url = f'{self.login_url}/oauth/access'
    # #     data = {'grant_type': 'authorization_code',
    # #             'code': code,
    # #             'redirect_uri': SONOS_REDIRECT_URI}
    # #     response = requests.post(url, data=data, auth=self.get_auth_header())
        
    # #     if response.ok:
    # #         token_info = {item: response.json()[item] for item in ['access_token' 'refresh_token', 'scopes']}
    # #         save_token(SONOS_TOKENS_FOLDER, user_email, token_info)
            
    def get_headers(self):
        headers = {'accept': 'application/json',
                   'authorization': f'Bearer {self.access_token}'}
        return headers
    
    def get_post_headers(self):
        headers = self.get_headers()
        headers['content-type'] = 'application/json'
        return headers
        
    def connect(self, user_email):
        token_info = get_token(SONOS_TOKENS_FOLDER, user_email)
        url = f'{self.login_url}/oauth/access'
        data = {'grant_type': 'refresh_token',
                'refresh_token': token_info['refresh_token']}
        response = requests.post(url, data=data, auth=self.get_auth_header())
        if response.ok:
            self.access_token = response.json()['access_token']
            
    def get_households(self):
        url = f'{self.control_url}/households'
        data = {'connectedOnly': True}
        response = requests.get(url, params=data, headers=self.get_headers(), timeout=10)
        if response.ok:
            household = response.json()['households'][0]
            self.household_id = household['id']
            self.owner_id = household['ownerLuid']
        else:
            print(f'Response failed: {response.status_code} - {response.reason}')
            
    def get_groups_and_players(self):
        url = f'{self.control_url}/households/{self.household_id}/groups'
        data = {}
        response = requests.get(url, params=data, headers=self.get_headers())
        if response.ok:        
            self.groups = {g['id']: {'name': g['name'],
                                     'coordinator_id': g['coordinatorId'],
                                     'player_ids': g['playerIds'],
                                     } for g in response.json()['groups']}
            self.players = {p['id']: {'name': p['name'],
                                      } for p in response.json()['players']}
            
            # find the most popular active or idle group
            sonos_states = ['PLAYBACK_STATE_PLAYING',
                            'PLAYBACK_STATE_PAUSED',
                            'PLAYBACK_STATE_IDLE']     
            playback_states = [(g['id'], g['playbackState'], len(g['playerIds'])) for g in response.json()['groups']]
            self.controller_id = sorted(playback_states, key = lambda x: (sonos_states.index(x[1]),
                                                                          -x[2]))[0][0]
            self.set_controller_name()
            
        else:
            print(f'Response failed: {response.status_code} - {response.reason}')
            
    def set_party_mode(self, volume=None):
        if len(self.groups) > 1:
            url = f'{self.control_url}/households/{self.household_id}/groups/createGroup'
            data = {'playerIds': [p for p in self.players],
                    'musicContextGroupId': self.controller_id
                    }
            response = requests.post(url, data=data, headers=self.get_headers())
            if response.ok:
                self.controller_id = response.json()['group_id']
                self.set_controller_name()

    def get_play_status(self):
        url = f'{self.control_url}/groups/{self.controller_id}/playback'
        response = requests.get(url, headers=self.get_headers())
        if response.ok:
            match response.json()['playbackState']:
                case 'PLAYBACK_STATE_PLAYING' | 'PLAYBACK_STATE_BUFFERING':
                    play_status = True
                case 'PLAYBACK_STATE_IDLE' | 'PLAYBACK_STATE_PAUSED':
                    play_status = False
            
        else:
            print(response.reason)
            play_status = None
            
        return play_status
        
    def change_play_status(self):
        url = f'{self.control_url}/groups/{self.controller_id}/playback/togglePlayPause'
        response = requests.post(url, headers=self.get_headers())
        if not response.ok:
            print(response.reason)
        
        return self.get_play_status()
                
    def set_controller_name(self):
        coordinator_id = self.groups[self.controller_id]['coordinator_id']
        self.controller_name = self.players[coordinator_id]['name']
        
    def set_household_volume(self, volumes):
        for player_id in self.players:
            discover_by_name(self.players[player_id]['name']).volume = volumes.get[player_id] # might need exception when player is new

    def get_household_volume(self):
        volumes = {player_id: discover_by_name(self.players[player_id]['name']).volume \
                   for player_id in self.players}
        
        return volumes
                
    def get_sonos_uris(self, service_name, track_uris):
        '''
        soundcloud -> 'x-sonos-http:track-%3esoundcloud%3atracks%3a{track_uri}.mp3?sid={160}&flags={8232}&sn={2}'
        onedrive ->  'x-sonos-http:{track_uri}.mp3?sid={248}&flags={8232}&sn={8}'
        spotify -> 'x-sonos-spotify:spotify%3atrack%3a{track_url}?sid={12}&flags={8232}&sn={1}'
        youtube -> 'x-sonosapi-hls-static:{ALkSOiEQuZL4AVQ8himx4Pm5kv9UTIBKifYsu6F9kymBFtZy}?sid={284}&flags={8}&sn={9}
        '''
        if service_name in self.callouts:
            match service_name:
                case 'YouTube':
                    x_sonos = 'sonosapi'
                case _:
                    x_sonos = 'sonos'
                    
            match service_name:
                case 'SoundCloud' | 'OneDrive':
                    extension = '.mp3'
                case _:
                    extension = ''
                    
            callout = self.callouts[service_name]
            
            sid = MusicService.get_data_for_name(service_name)['ServiceID']
        
            sonos_uris = [f'x-{x_sonos}-{callout}{track_uri}{extension}?sid={sid}' for track_uri in track_uris]
            
        else:
            print(f'{service_name} is not yet supported.')
            sonos_uris = None
        
        return sonos_uris
    
    def play_release(self, service_name, track_uris):
        uris = self.get_sonos_uris(service_name, track_uris)
        if len(uris):
            device = discover_by_name(self.controller_name)
            device.clear_queue()
        
            for uri in uris:
                device.add_uri_to_queue(uri)
        
            device.play_from_queue(0)
            
    def create_session(self):
        url = f'{self.control_url}/households/{self.household_id}/groups'
        response = requests.get(url, headers=self.get_headers())
        print(f'{response=}')

        url = f'{self.control_url}/groups/{parse.quote(self.controller_id)}/playbackSession'
        data = {'appId': 'com.digitalvinyls',
                'appContext': 'digitalvinyls'}
        response = requests.post(url, json=data, headers=self.get_post_headers()) #, data=data, headers=self.get_headers())
        if response.ok:
            items = response.json()
            session_id = items['sessionId']
            session_state = items['sessionState']
            session_created = items['sessionCreated']
            
            print(f'{items=}')
        
            return session_id
        else:
            print(self.groups)
            print(response.url)
            print(response.status_code)
            print(response.reason)
    
    def add_uri_to_session(self, session_id, track_uri, play=False):
        url = f'{self.control_url}/groups/{self.controller_id}/playbackSession/{session_id}/loadCloudQueue'
        data = {'queueData': track_uri,
                'playOnCompletion': play}
        response = requests.post(url, json=data, headers=self.get_post_headers())
        print(self.players)
        print(self.groups)
        print(f'{response=}')
        print(f'{response.url=}')
        print(f'{data=}')

        url = f'{self.control_url}/sessions/{session_id}/playbackSession/loadCloudQueue'
        data = {'queueBaseUrl': track_uri,
                'playOnCompletion': play}
        response = requests.post(url, json=data, headers=self.get_post_headers())
        print(self.players)
        print(self.groups)
        print(f'{response=}')
        print(f'{response.url=}')
        print(f'{data=}')
        

    # # def get_playlists(self):
    # #     url = f'{self.control_url}/households/{self.household_id}/playlists'
    # #     data = {}S
    # #     response = requests.get(url, params=data, headers=self.get_headers())
    # #     if response.ok:
    # #         print(response.json())
            
    # # def get_favorites(self):
    # #     url = f'{self.control_url}/households/{self.household_id}/favorites'
    # #     data = {}
    # #     response = requests.get(url, params=data, headers=self.get_headers())
    # #     if response.ok:
    # #         print(response.json())
    # #         items = response.json()['items']
    # #         info_albums = {}
    # #         info_artists = {}
    # #         info_ownerships = {}
            
    # #         #info_albums['artist_uris'] = [None for item in items]
    # #         info_albums['album_uri'] = [item['id'] for item in items]
    # #         info_albums['album_name'] = [item['name'] for item in items]
    # #         info_albums['image_src'] = [item['images'][0]['url'] for item in items]
    # #         info_albums['album_type'] = [item['resource']['type'].lower() for item in items]
    # #         #info_albums['track_uris'] = [None for item in items]
    # #         #info_albums['album_duration'] = [None for item in items]
    # #         #info_albums['upc'] = [None for item in items]
    # #         #info_albums['release_date'] = [None for item in items]

    # #         #info_artists['artist_uri'] = [None for item in items]
    # #         #info_artists['artist_name'] = [None for item in items]

    # #         info_ownerships['album_uri'] = info_albums['album_uri']
    # #         info_ownerships['like_date'] = datetime.today().date()
    
    # #     albums_df = DataFrame(info_albums)
    # #     artists_df = DataFrame(info_artists).drop_duplicates()
    # #     ownerships_df = DataFrame(info_ownerships)
        
    # #     return albums_df, artists_df, ownerships_df
    
    # # def play_sonos_list(self, list_name):
    # #     list_source = None
    # #     if list_name in self.favorites.values():
    # #         list_source = 'favorites'
    # #         source = self.favorites
            
    # #     elif list_name in self.playlists.values():
    # #         list_source = 'playlists'
    # #         source = self.playlists
            
    # #     if list_source:
    # #         list_id = next(key for key, value in source.items() if value == list_name)
    # #         url = f'{self.control_url}/groups/{self.controller_id}/{list_source}'
    # #         data = {f'{list_source[:-1]}Id': list_id,
    # #                 'action': 'REPLACE',
    # #                 'playOnCompletion': True}

    # #         response = requests.post(url, data=data, headers=self.get_headers())
    # #         if response.ok:
    # #             pass
        

