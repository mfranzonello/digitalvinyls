''' Personal account variables '''

import json
from os.path import dirname, realpath

SPOTIFY_AUTHS_FOLDER = 'auths/spotify'
SPOTIFY_TOKENS_FOLDER = f'{SPOTIFY_AUTHS_FOLDER}/tokens'
SONOS_AUTHS_FOLDER = 'auths/sonos'
SONOS_TOKENS_FOLDER = f'{SONOS_AUTHS_FOLDER}/tokens'
STRUCTURE_FOLDER = 'jsons'
CSVS_FOLDER = 'csvs'
CRITICS_FOLDER = f'{CSVS_FOLDER}/critics'

def read_json(filepath, filename):
    with open(dirname(dirname(realpath(__file__))) + '/' + filepath + '/' + filename + '.json') as f:
        json_dict = json.load(f)
        
    return json_dict

def write_json(filepath, filename, json_dict):
    with open(dirname(dirname(realpath(__file__))) + '/' + filepath + '/' + filename + '.json') as f:
        json.dump(json_dict, f)

structures = read_json(STRUCTURE_FOLDER, 'structure')

SPOTIFY_USER_ID = structures['SPOTIFY_USER_ID']
SPOTIFY_USERNAME = structures['SPOTIFY_USERNAME']
SONOS_REDIRECT_URI = structures['SONOS_REDIRECT_URI']
SPOTIFY_REDIRECT_URI = structures['SONOS_REDIRECT_URI']
NEON_DB_NAME = structures['NEON_DB_NAME']
NEON_USERNAME = structures['NEON_USERNAME']
NEON_HOST = structures['NEON_HOST']
SONOS_HOST = structures['SONOS_HOST']
SONOS_PORT = structures['SONOS_PORT']

def get_scope(folder):
    scope = read_json(folder, 'scopes')
    return ', '.join(scope)