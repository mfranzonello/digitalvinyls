''' Personal account variables '''

import json
import yaml
from os import makedirs
from os.path import dirname, realpath, join as pathjoin

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

class VALUES:
    AUTHS_FOLDER = 'auths'
    TOKENS_FOLDER = 'tokens'
    SPOTIFY_TOKENS_FOLDER = f'{AUTHS_FOLDER}/spotify/{TOKENS_FOLDER}'
    SONOS_TOKENS_FOLDER = f'{AUTHS_FOLDER}/sonos/{TOKENS_FOLDER}'
    AZURE_TOKENS_FOLDER = f'{AUTHS_FOLDER}/azure/{TOKENS_FOLDER}'
    CONFIG_FOLDER = 'config'
    CSVS_FOLDER = 'csvs'
    CRITICS_FOLDER = f'{CSVS_FOLDER}/critics'

    config = read_yaml(CONFIG_FOLDER, 'config')
    SPOTIFY_REDIRECT_URI = config['spotify']['redirect_uri']
    SPOTIFY_SCOPE = config['spotify']['scope']
    SPOTIFY_PLAYLIST_WORD = config['spotify']['vinyl_playlist_word']

    SONOS_REDIRECT_URI = config['sonos']['redirect_uri']
    SONOS_HOST = config['sonos']['host']
    SONOS_PORT = config['sonos']['port']

    NEON_DB_NAME = config['neon']['db_name']
    NEON_USERNAME = config['neon']['username']
    NEON_HOST = config['neon']['host']

    AZURE_REDIRECT_URI = config['azure']['redirect_uri']
    AZURE_SCOPE = config['azure']['scope']
    AZURE_TENANT_ID = config['azure']['tenant_id']
    AZURE_VINYLS_FOLDER = config['azure']['vinyl_folder_path']
    
    ##BILLBOARD_CHART_START = config['billboard']['chart_start']
    
class LOCATIONS:
    CONFIG_FOLDER = 'config'
    config = read_yaml(CONFIG_FOLDER, 'api')
    
    GITHUB_URL = config['github']['urls']['env']
    SPOTIFY_PLAY_URL = config['spotify']['urls']['play']
    SPOTIFY_AUTH_URL = config['spotify']['urls']['auth']
    SPOTIFY_LOGIN_URL = config['spotify']['urls']['login']
    MUSICBRAINZ_URL = config['musicbrainz']['urls']['search']
    SONOS_LOGIN_URL = config['sonos']['urls']['login']
    SONOS_CONTROL_URL = config['sonos']['urls']['control']
    SOUNDCLOUD_PLAY_URL = config['soundcloud']['urls']['play']
    SOUNDCLOUD_WIDGET_URL = config['soundcloud']['urls']['widget']
    AZURE_LOGIN_URL = config['azure']['urls']['login']
    AZURE_GRAPH_URL = config['azure']['urls']['graph']