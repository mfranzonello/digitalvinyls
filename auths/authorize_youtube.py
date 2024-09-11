''' Authenticate a user to access YouTube Music '''

import os

from flask import redirect, request, session, url_for, render_template, Blueprint
from google_auth_oauthlib.flow import Flow
import googleapiclient.discovery

from ..common.tokens import get_token, save_token, update_profile
from ..common.structure import PROFILES_FOLDER, YOUTUBE_REDIRECT_URI, YOUTUBE_SCOPE, YOUTUBE_TOKENS_FOLDER
from ..common.secret import get_secret

youtube_auth = Blueprint('youtube_auth', __name__)

# Allow insecure transport for local development
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

def get_config():
    config = {'web': {'client_id': get_secret('YOUTUBE_CLIENT_ID'),
                      'client_secret': get_secret('YOUTUBE_CLIENT_SECRET'),
                      'redirect_uris': [YOUTUBE_REDIRECT_URI],
                      'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
                      'token_uri': 'https://oauth2.googleapis.com/token',
                      },
              }        
    return config
        
# Flask routes for YouTube OAuth2
@youtube_auth.route('/<email>')
def authorize(email):
    flow = Flow.from_client_config(client_config=get_config(), scopes=YOUTUBE_SCOPE, redirect_uri=YOUTUBE_REDIRECT_URI)
    ##flow.redirect_uri = REDIRECT_URI
    authorization_url, state = flow.authorization_url(access_type='offline',
                                                      include_granted_scopes='true')
    session['state'] = state
    session['profile_email'] = email
    return redirect(authorization_url)

@youtube_auth.route('/callback')
def callback():
    state = session.get('state')
    flow = Flow.from_client_config(client_config=get_config(), scopes=YOUTUBE_SCOPE, redirect_uri=YOUTUBE_REDIRECT_URI,
                                   state=state)

    authorization_response = request.url
    flow.fetch_token(authorization_response=authorization_response)

    credentials = flow.credentials
    youtube = googleapiclient.discovery.build('youtube', 'v3', credentials=credentials)

    user_info = youtube.channels().list(mine=True, part="snippet").execute()
    first_name = user_info['items'][0]['snippet']['title']
    user_id = user_info['items'][0]['id']

    creds_data = {'token': credentials.token,
                  'refresh_token': credentials.refresh_token,
                  'token_uri': credentials.token_uri,
                  'scopes': credentials.scopes,
                  'user_info': user_info
                  }
    profile_data = {'youtube': user_id}
    
    save_token(YOUTUBE_TOKENS_FOLDER, user_id, creds_data)
    update_profile(PROFILES_FOLDER, session['profile_email'], profile_data)
  
    return redirect(url_for('profiles')) 
