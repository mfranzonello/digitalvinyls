''' Authenticate a user to access Spotify '''

import os
import base64
import random
import string
from urllib.parse import urlencode

import requests
from flask import request, redirect, make_response, url_for, session, Blueprint

from ..common.secret import get_secret
from ..common.tokens import get_token, save_token, update_profile
from ..common.structure import PROFILES_FOLDER, SPOTIFY_AUTH_URL, SPOTIFY_SEARCH_URL, SPOTIFY_REDIRECT_URI, SPOTIFY_SCOPE, SPOTIFY_TOKENS_FOLDER

spotify_auth = Blueprint('spotify_auth', __name__)

# Configurations
client_id = get_secret('SPOTIFY_CLIENT_ID')
client_secret = get_secret('SPOTIFY_CLIENT_SECRET')
redirect_uri = SPOTIFY_REDIRECT_URI
scope = ' '.join(SPOTIFY_SCOPE)

state_key = 'spotify_auth_state'

def generate_random_string(length):
    possible = string.ascii_letters + string.digits
    return ''.join(random.choice(possible) for _ in range(length))

@spotify_auth.route('/<email>')
def authorize(email):
    state = generate_random_string(16)
    url = f'{SPOTIFY_AUTH_URL}/authorize'
    data = {'response_type': 'code',
            'client_id': client_id,
            'scope': scope,
            'redirect_uri': redirect_uri,
            'state': state,
            'show_dialog': True
            }
    redirect_url = f'{url}?{urlencode(data)}'
    resp = make_response(redirect(redirect_url))

    session['state'] = state
    session['state_key'] = state_key
    session['profile_email'] = email

    return resp

@spotify_auth.route('/callback')
def callback():
    code = request.args.get('code')
    state = request.args.get('state')
    stored_state = session['state']

    if state is None or state != stored_state:
        print(f'{state=}')
        print(f'{stored_state=}')
        return redirect('/#?error=state_mismatch')
    else:
        data = {'code': code,
                'redirect_uri': redirect_uri,
                'grant_type': 'authorization_code'}
        headers = {'Authorization': 'Basic ' + base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()}
        response = requests.post(url=f'{SPOTIFY_AUTH_URL}/api/token', data=data, headers=headers)
        
        if response.ok:
            response_data = response.json()
            access_token = response_data.get('access_token')
            refresh_token = response_data.get('refresh_token')
           
            response = requests.get(f'{SPOTIFY_SEARCH_URL}/me', headers={'Authorization': f'Bearer {access_token}'})
           
            user_data = response.json()
            
            spot_user = {'display_name': user_data.get('display_name'),
                         'refresh_token': refresh_token,
                         'scope': scope,
                         'image_src': user_data.get('images', [{}])[0].get('url', '')
                         }
            profile_data = {'spotify': user_data['id']}

            save_token(SPOTIFY_TOKENS_FOLDER, user_data['id'], spot_user)
            update_profile(PROFILES_FOLDER, session['profile_email'], profile_data)

            return redirect(url_for('profiles'))

        else:
            return redirect('/#?error=invalid_token')
        
@spotify_auth.route('/delete/<email>')
def delete(email):
    update_profile(PROFILES_FOLDER, email, 'spotify')
    return redirect(url_for('profiles'))