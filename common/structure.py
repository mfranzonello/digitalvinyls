''' Personal account variables '''

import json
import yaml
from os.path import dirname, realpath

def read_yaml(filepath, filename):
    with open(dirname(dirname(realpath(__file__))) + '/' + filepath + '/' + filename + '.yaml', 'r') as file:
        config = yaml.safe_load(file)
        
    return config

def read_json(filepath, filename):
    with open(dirname(dirname(realpath(__file__))) + '/' + filepath + '/' + filename + '.json', 'r') as f:
        json_dict = json.load(f)
        
    return json_dict

def write_json(filepath, filename, json_dict):
    with open(dirname(dirname(realpath(__file__))) + '/' + filepath + '/' + filename + '.json', 'w') as f:
        json.dump(json_dict, f)

def get_scope(folder):
    scope = read_json(folder, 'scopes')
    return ', '.join(scope)

_auths_folder = 'auths'
_tokens_folder = 'tokens'
_config_folder = 'config'
_csvs_folder = 'csvs'

_config = read_yaml(_config_folder, 'config')
_api = read_yaml(_config_folder, 'api')
    
# LOCATIONS:
GITHUB_URL = _api['github']['urls']['env']

SPOTIFY_PLAY_URL = _api['spotify']['urls']['play']
SPOTIFY_AUTH_URL = _api['spotify']['urls']['auth']
SPOTIFY_LOGIN_URL = _api['spotify']['urls']['login']

MUSICBRAINZ_URL = _api['musicbrainz']['urls']['search']

SONOS_LOGIN_URL = _api['sonos']['urls']['login']
SONOS_CONTROL_URL = _api['sonos']['urls']['control']

SOUNDCLOUD_PLAY_URL = _api['soundcloud']['urls']['play']
SOUNDCLOUD_WIDGET_URL = _api['soundcloud']['urls']['widget']

AZURE_LOGIN_URL = _api['azure']['urls']['login']
AZURE_GRAPH_URL = _api['azure']['urls']['graph']

# VALUES
SPOTIFY_TOKENS_FOLDER = f'{_auths_folder}/spotify/{_tokens_folder}'
SONOS_TOKENS_FOLDER = f'{_auths_folder}/sonos/{_tokens_folder}'
AZURE_TOKENS_FOLDER = f'{_auths_folder}/azure/{_tokens_folder}'

CRITICS_FOLDER = f'{_csvs_folder}/critics'

SPOTIFY_REDIRECT_URI = _config['spotify']['redirect_uri']
SPOTIFY_SCOPE = _config['spotify']['scope']
SPOTIFY_PLAYLIST_WORD = _config['spotify']['vinyl_playlist_word']

SONOS_REDIRECT_URI = _config['sonos']['redirect_uri']
SONOS_HOST = _config['sonos']['host']
SONOS_PORT = _config['sonos']['port']

NEON_DB_NAME = _config['neon']['db_name']
NEON_USERNAME = _config['neon']['username']
NEON_HOST = _config['neon']['host']

AZURE_REDIRECT_URI = _config['azure']['redirect_uri']
AZURE_SCOPE = _config['azure']['scope']
AZURE_TENANT_ID = _config['azure']['tenant_id']
AZURE_VINYLS_FOLDER = _config['azure']['vinyl_folder_path']