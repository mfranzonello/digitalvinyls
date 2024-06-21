''' Devices to play music '''

#from base64 import b64encode

import requests
from requests.auth import HTTPBasicAuth
import soco
from soco.data_structures import DidlItem, to_didl_string
from soco.discovery import by_name as discover_by_name
from soco.music_services import MusicService

from common.calling import Caller
from common.structure import SONOS_TOKENS_FOLDER, SONOS_REDIRECT_URI
from common.locations import SONOS_HOST, SONOS_PORT
from common.secret import get_secret, get_token, save_token

class Sonoser(Caller):
    login_url = 'https://api.sonos.com/login/v3'
    control_url = 'https://api.ws.sonos.com/control/api/v1/'
    def __init__(self):
        super().__init__()
        self.access_token = None
        
        self.household_id = None
        self.owner_id = None
        self.groups = []
        self.controller_id = None
        self.devices = []
        self.controller_name = None
        #self.session_id = None
        
    def get_auth_header(self):
        auth_header = HTTPBasicAuth(get_secret('SONOS_CLIENT_KEY'), get_secret('SONOS_CLIENT_SECRET'))
        return auth_header

    def get_access(self, user_email):
        code = get_token(SONOS_TOKENS_FOLDER, user_email)['code']
        url = f'{self.login_url}/oauth/access'
        data = {'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': SONOS_REDIRECT_URI}
        response = requests.post(url, data=data, auth=self.get_auth_header())
        
        if response.ok:
            token_info = {item: response.json()[item] for item in ['access_token' 'refresh_token', 'scopes']}
            save_token(SONOS_TOKENS_FOLDER, user_email, token_info)
            
    def get_headers(self):
        headers = {'accept': 'application/json',
                   'authorization': f'Bearer {self.access_token}'}
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
        response = requests.get(url, data=data, headers=self.get_headers())
        if response.ok:
            household = response.json()['households'][0]
            self.household_id = household['id']
            self.owner_id = household['ownerLuid']
        else:
            print(f'Response failed: {response.status_code} - {response.reason}')
            
    def get_groups_and_players(self):
        url = f'{self.control_url}/households/{self.household_id}/groups'
        data = {}
        response = requests.get(url, data=data, headers=self.get_headers())
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
            data = {'playerIds': [p['id'] for p in self.players],
                    'musicContextGroupId': self.controller_id
                    }
            response = requests.post(url, data=data, headers=self.get_headers())
            if response.ok:
                self.controller_id = response.json()['group_id']
                self.set_controller_name()
                
    def set_controller_name(self):
        coordinator_id = self.groups[self.controller_id]['coordinator_id']
        self.controller_name = self.players[coordinator_id]['name']

    def get_playlists(self):
        url = f'{self.control_url}/households/{self.household_id}/playlists'
        data = {}
        response = requests.get(url, data=data, headers=self.get_headers())
        if response.ok:
            self.playlists = {p['id']: p['name'] for p in response.json()['playlists']}

    def play_sonos_list(self, list_name):
        list_source = None
        if list_name in self.favorites.values():
            list_source = 'favorites'
            source = self.favorites
            
        elif list_name in self.playlists.values():
            list_source = 'playlists'
            source = self.playlists
            
        if list_source:
            list_id = next(key for key, value in source.items() if value == list_name)
            url = f'{self.control_url}/groups/{self.groups[0]["id"]}/{list_source}'
            data = {f'{list_source[:-1]}Id': list_id,
                    'action': 'REPLACE',
                    'playOnCompletion': True}

            response = requests.post(url, data=data, headers=self.get_headers())
            if response.ok:
                pass
        
    # # def play_spotify_album(self, track_list):
    # #     requests.get(url=f'{SONOS_HOST}:{SONOS_PORT}/clearqueue')
    # #     for i, track_uri in enumerate(track_list):
    # #         n = 'now' if i == 0 else 'next'
    # #         requests.get(url=f'{SONOS_HOST}:{SONOS_PORT}/{self.controller_name}/spotify/{n}/spotify:track:{track_uri}')
            
    def play_spotify_album(self, track_list, titles):
        ms = MusicService('Spotify')
        service_type = ''
        service_token = f'SA_RINCON${service_type}_X_#Svc${service_type}-0-Token'
        spotifyDef = {'metastart': {'album':   '0004206cspotify%3aalbum%3a',
                                    'track':    '00032020spotify%3atrack%3a',
                                    #'station': '000c206cspotify:artistRadio%3a',
                                    #'playlist': '0004206cspotify%3aplaylist%3a'
                                    },
                      'parent':    {'album':   '00020000album:',
                                    'track':    '00020000track:',
                                    #'station': '00052064spotify%3aartist%3a',
                                    #'playlist':'00020000playlist:',
                                    },
                      'object':    {'album':   'container.album.musicAlbum',
                                    'track':    'item.audioItem.musicTrack',
                                    #'station': 'item.audioItem.audioBroadcast.#artistRadio',
                                    #'playlist':'container.playlistContainer',
                                    },
             
                      }        
        #tag_0 = 'x-rincon-cpcontainer:0004206c'
        # # tags = {'spotify': ['x-sonos-spotify:spotify%3atrack%3a', '?sid=12&flags=8232&sn=1'],
        # #         'soundcloud': ['x-sonos-http:track-%3esoundcloud%3atracks%3', '?sid=160&flags=8232&sn=2'],
        # #         }               
        tags = ['x-sonos-spotify:spotify%3atrack%3a', '?sid=12&flags=8232&sn=1']
        
        device = discover_by_name(self.controller_name)
        device.clear_queue()
        
        for track_uri, title in zip(track_list, titles):
            uri = f'{tags[0]}{track_uri}{tags[1]}'
            title = 'HEY'
            item_id = f'spotify:track:{track_uri}'
            parent_id = spotifyDef['parent']['album'] + 'HO'
            didl = DidlItem(title=title, parent_id=parent_id, item_id=item_id, desc=ms.desc)
            # # if i == 0:
            # #     device.play_uri(uri, meta=to_didl_string(didl))
            # # else:
            device.add_uri_to_queue(uri, meta=to_didl_string(didl))#, position=i+1, as_next=True)
        
        device.play_from_queue(0)