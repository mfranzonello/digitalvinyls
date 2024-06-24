''' Devices to play music '''

#from base64 import b64encode

import requests
from requests.auth import HTTPBasicAuth
from soco.data_structures import DidlItem, to_didl_string
from soco.discovery import by_name as discover_by_name
from soco.music_services import MusicService

from common.calling import Caller
from common.locations import SONOS_LOGIN_URL, SONOS_CONTROL_URL
from common.structure import SONOS_TOKENS_FOLDER, SONOS_REDIRECT_URI, SONOS_HOST, SONOS_PORT
from common.secret import get_secret, get_token, save_token
from common.entry import Stroker
from music.liseners import Picker

class Sonoser(Caller):
    login_url = SONOS_LOGIN_URL
    control_url = SONOS_CONTROL_URL
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
            data = {'playerIds': [p['id'] for p in self.players],
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

    def get_playlists(self):
        url = f'{self.control_url}/households/{self.household_id}/playlists'
        data = {}
        response = requests.get(url, params=data, headers=self.get_headers())
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
            url = f'{self.control_url}/groups/{self.controller_id}/{list_source}'
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

class Turntable(Picker):
    def __init__(self):
        super().__init__()
        self.users = []
        self.record_stack = []
        self.needle = -1
        
    def play_music(self, neon, sonoser):
        user = self.select_user(message='listen to')   

        user_name = user.first_name + ' ' + user.last_name
        user_id = user.user_id
        print(f'Welcome {user_name}!')
        press_right = '[→] to play the next album'
        
        loop = True
        while loop:
            if self.needle >= 0:
                press_left = '[←] to go back to the previous album'
                press_up = '[↑] to replay this album'
                press_down = f'[↓] to {"pause" if sonoser.get_play_status() else "continue"} playing'
                allowed_keys = ['LEFT', 'RIGHT', 'UP', 'DOWN']
            else:
                press_left = press_up = press_down = None
                allowed_keys = ['RIGHT']
                
            press_choices = ', '.join(p for p in [press_left, press_up, press_down, press_right] if p)
            print(f'Press {press_choices} or [Q] to quit.')
            key, loop = Stroker.get_keystroke(allowed_keys=allowed_keys, quit_key='Q')
            if loop:
                match key:
                    case 'LEFT':
                        skip = -1
                    case 'RIGHT':
                        skip = 1
                    case 'UP':
                        skip = 0
                    
                if key in ['LEFT', 'RIGHT', 'UP']:
                    album_s = self.select_album(neon, user_id, skip)
                    self.play_album(neon, sonoser, user_id, album_s)

                elif key == 'DOWN':
                    sonoser.change_play_status()
                
    def select_album(self, neon, user_id, skip=0):
        # get the next album to play
        self.needle += skip
        if self.needle >= len(self.record_stack):
            # add new album to the stack
            self.record_stack.append(neon.get_random_album(user_id))
        album_s = self.record_stack[self.needle]
        return album_s
        
    def play_album(self, neon, sonoser, user_id, album_s):
        artist_names, album_name = album_s[['artist_names', 'album_name']]
        service_name, source_name, service_id, source_id = album_s[['service_name', 'source_name',
                                                                    'service_id', 'source_id']]
    
        print(f'Playing {artist_names} - {album_name} from {service_name}')
        match service_id:
            case 1: # Spotify
                match source_id:
                    case 1:
                        sonoser.play_spotify_album(album_s['track_list'], titles=['']*len(album_s['track_list'])) ## need titles
                    case _:
                        print(f'{source_name} is not supported yet')
            case 2: # Soundcloud
                print('This service type is not supported yet')
            case 3: # Sonos
                print('This service type is not supported yet')
            case _:
                print('This service type is not supported yet')