''' Personal account variables '''

import json
from os.path import dirname, realpath

SPOTIFY_AUTHS_FOLDER = 'auths/spotify'
SPOTIFY_TOKENS_FOLDER = f'{SPOTIFY_AUTHS_FOLDER}/tokens'
STRUCTURE_FOLDER = 'jsons'

def read_json(filepath, filename):
    with open(dirname(dirname(realpath(__file__))) + '/' + filepath + '/' + filename + '.json') as f:
        json_dict = json.load(f)
        
    return json_dict

structures = read_json(STRUCTURE_FOLDER, 'structure')

SPOTIFY_USER_ID = structures['SPOTIFY_USER_ID']
SPOTIFY_USERNAME = structures['SPOTIFY_USERNAME']

def get_token(user_id):
    token_info = read_json(SPOTIFY_TOKENS_FOLDER, user_id)
    return token_info

def get_scope():
    scope = read_json(SPOTIFY_AUTHS_FOLDER, 'scopes')
    return ', '.join(scope)