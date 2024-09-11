''' Authenticate a user to access SoundCloud '''

import re

import requests
from flask import redirect, request, session, url_for, render_template, Blueprint

from ..common.structure import PROFILES_FOLDER, SOUNDCLOUD_PLAY_URL, SOUNDCLOUD_SEARCH_URL, SOUNDCLOUD_TOKENS_FOLDER
from ..common.tokens import save_token, update_profile

soundcloud_auth = Blueprint('soundcloud_auth', __name__)

@soundcloud_auth.route('/<email>', methods=['GET', 'POST'])
def authorize(email):
    match request.method:
        case 'GET':
            session['profile_email'] = email
            return render_template('authorize.html', service_name='SoundCloud')
        
        case 'POST':
            username = request.form.get('username')
            response = requests.get(f'{SOUNDCLOUD_PLAY_URL}/{username}')
            user_search = re.search('"uri":"https://api.soundcloud.com/users/(\d+)', response.text) 
            
            if user_search:
                # found a match
                user_uri = user_search.group(1)
                user_data = {'username': username, 'user_id': user_uri}
                profile_data = {'soundcloud': user_uri}
            
                save_token(SOUNDCLOUD_TOKENS_FOLDER, user_uri, user_data)
                update_profile(PROFILES_FOLDER, session['profile_email'], profile_data)

                return redirect(url_for('profiles'))
            
            else:
                # no match found
                return 'No match found', 400
