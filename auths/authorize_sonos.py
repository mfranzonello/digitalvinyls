''' Authenticate a user to access Spotify '''

import random
import string
from urllib.parse import urlencode

import requests
from requests.auth import HTTPBasicAuth
from flask import request, redirect, make_response, url_for, session, Blueprint

from ..common.secret import get_secret
from ..common.tokens import save_token, update_profile
from ..common.structure import PROFILES_FOLDER, SONOS_AUTH_URL, SONOS_CONTROL_URL, SONOS_REDIRECT_URI, SONOS_SCOPE, SONOS_TOKENS_FOLDER

sonos_auth = Blueprint('sonos_auth', __name__)

state_key = 'sonos_auth_state'

def generate_random_string(length):
    possible = string.ascii_letters + string.digits
    return ''.join(random.choice(possible) for _ in range(length))

@sonos_auth.route('/<email>')
def authorize(email):
    state = generate_random_string(16)
    url = f'{SONOS_AUTH_URL}/oauth'
    data = {'response_type': 'code',
            'client_id': get_secret('SONOS_CLIENT_KEY'),
            'scope': ' '.join(SONOS_SCOPE),
            'redirect_uri': SONOS_REDIRECT_URI,
            'state': state,
            }
    redirect_url = f'{url}?{urlencode(data)}'
    resp = make_response(redirect(redirect_url))
    session['state'] = state
    session['state_key'] = state_key
    session['profile_email'] = email

    return resp

@sonos_auth.route('/callback')
def callback():
    code = request.args.get('code')
    state = request.args.get('state')
    stored_state = session['state']

    if state is None or state != stored_state:
        return redirect('/#?error=state_mismatch')
    else:
        data = {'code': code,
                'redirect_uri': SONOS_REDIRECT_URI,
                'grant_type': 'authorization_code'}
        headers = HTTPBasicAuth(get_secret('SONOS_CLIENT_KEY'), get_secret('SONOS_CLIENT_SECRET'))
        response = requests.post(url=f'{SONOS_AUTH_URL}/oauth/access', data=data, headers=headers)
        
        if response.ok:
            response_data = response.json()
            access_token = response_data.get('access_token')
            refresh_token = response_data.get('refresh_token')
            
            # get the household_id, which is the equivalent of user_id
            headers = {'accept': 'application/json',
                       'authorization': f'Bearer {access_token}'}
            data = {'connectedOnly': True}
            response = requests.get(f'{SONOS_CONTROL_URL}/households', params=data, headers=headers)
            
            household_id = response.json()['households'][0]['id']

            user_info = {'refresh_token': refresh_token,
                            'scope': SONOS_SCOPE,
                            }
            profile_data = {'sonos': household_id}

            save_token(SONOS_TOKENS_FOLDER, household_id, user_info)
            update_profile(PROFILES_FOLDER, session['profile_email'], profile_data)

            return redirect(url_for('profiles'))
            
        else:
            return redirect('/#?error=invalid_token')