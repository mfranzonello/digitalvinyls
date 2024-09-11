''' Project specific variables from YAML files '''

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
_tokens_folder = f'{_auths_folder}/tokens'
_config_folder = 'config'
_csvs_folder = 'csvs'

_config = read_yaml(_config_folder, 'config')
_api = read_yaml(_config_folder, 'api')
    
PROFILES_FOLDER = f'{_auths_folder}/app'

# LOCATIONS:
GITHUB_URL = _api['github']['urls']['env']

SPOTIFY_AUTH_URL = _api['spotify']['urls']['auth']
SPOTIFY_SEARCH_URL = _api['spotify']['urls']['search']

MUSICBRAINZ_URL = _api['musicbrainz']['urls']['search']

SONOS_AUTH_URL = _api['sonos']['urls']['auth']
SONOS_CONTROL_URL = _api['sonos']['urls']['control']

SOUNDCLOUD_PLAY_URL = _api['soundcloud']['urls']['play']
SOUNDCLOUD_SEARCH_URL = _api['soundcloud']['urls']['search']

AZURE_LOGIN_URL = _api['azure']['urls']['login']
AZURE_GRAPH_URL = _api['azure']['urls']['graph']

LAST_FM_SEARCH_URL = _api['lastfm']['urls']['search']

GCP_AUTH_URL = _api['gcp']['urls']['auth']
GCP_APIS_URL = _api['gcp']['urls']['apis']
GCP_TOKEN_URI = _api['gcp']['urls']['token']


# LIMITS
SPOTIFY_RATE_LIMIT = _api['spotify']['limits']['rate']
SPOTIFY_QUERY_LIMIT = _api['spotify']['limits']['query']

SOUNDCLOUD_RATE_LIMIT = _api['soundcloud']['limits']['rate']
SOUNDCLOUD_QUERY_LIMIT = _api['soundcloud']['limits']['query']

AZURE_RATE_LIMIT = _api['azure']['limits']['rate']

MUSICBRAINZ_RATE_LIMIT = _api['musicbrainz']['limits']['rate']
MUSICBRAINZ_QHAR_LIMIT = _api['musicbrainz']['limits']['qhar']

BILLBOARD_RATE_LIMIT = _api['billboard']['limits']['rate']

LAST_FM_RATE_LIMIT = _api['billboard']['limits']['rate']


# VALUES
SPOTIFY_TOKENS_FOLDER = f'{_tokens_folder}/spotify'
SONOS_TOKENS_FOLDER = f'{_tokens_folder}/sonos'
AZURE_TOKENS_FOLDER = f'{_tokens_folder}/azure'
SOUNDCLOUD_TOKENS_FOLDER = f'{_tokens_folder}/soundcloud'
YOUTUBE_TOKENS_FOLDER = f'{_tokens_folder}/youtube'

CRITICS_FOLDER = f'{_csvs_folder}/critics'

SPOTIFY_REDIRECT_URI = _config['spotify']['redirect_uri']
SPOTIFY_SCOPE = _config['spotify']['scope']
SPOTIFY_PLAYLIST_WORD = _config['spotify']['vinyl_playlist_word']

SONOS_REDIRECT_URI = _config['sonos']['redirect_uri']
SONOS_SCOPE = _config['sonos']['scope']
SONOS_HOST = _config['sonos']['host']
SONOS_PORT = _config['sonos']['port']

NEON_DB_NAME = _config['neon']['db_name']
NEON_USERNAME = _config['neon']['username']
NEON_HOST = _config['neon']['host']

AZURE_REDIRECT_URI = _config['azure']['redirect_uri']
AZURE_SCOPE = _config['azure']['scope']
AZURE_TENANT_ID = _config['azure']['tenant_id']
AZURE_VINYLS_FOLDER = _config['azure']['vinyl_folder_path']

GCP_S_PROJECT_ID = _config['gcp']['project_id']
GCP_S_ACCOUNT_NAME = _config['gcp']['account_name']
GCP_S_BUCKET_NAME = _config['gcp']['bucket_name']

YOUTUBE_REDIRECT_URI = _config['youtube']['redirect_uri']
YOUTUBE_SCOPE = _config['youtube']['scope']