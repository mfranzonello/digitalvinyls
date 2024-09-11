''' Authenticate a user to access OneDrive '''

import requests
import time

import requests
from flask import request, redirect, session, Blueprint
import msal

from ..common.secret import get_secret
from ..common.structure import PROFILES_FOLDER, AZURE_REDIRECT_URI, AZURE_TENANT_ID, AZURE_SCOPE, AZURE_TOKENS_FOLDER
from ..common.tokens import save_token, update_profile

azure_auth = Blueprint('azure_auth', __name__)

# Azure AD app registration configuration
CLIENT_ID = get_secret('AZURE_CLIENT_ID')
CLIENT_SECRET = get_secret('AZURE_CLIENT_SECRET')
SCOPE = AZURE_SCOPE

# Redirect URI for the application (must be registered in Azure AD)
REDIRECT_PATH = '/getAToken' #AZURE_REDIRECT_URI remove localhost

# Azure AD app registration configuration
AUTHORITY = f'https://login.microsoftonline.com/common' ## move this

# Create the MSAL public client application
public_client = msal.PublicClientApplication(client_id=CLIENT_ID,
                                             authority=AUTHORITY)

@azure_auth.route('/<email>')
def authorize(email):
    session['profile_email'] = email
    
    # Construct the authorization request URL
    auth_url = public_client.get_authorization_request_url(scopes=SCOPE, redirect_uri=AZURE_REDIRECT_URI)
    return redirect(auth_url)

@azure_auth.route(REDIRECT_PATH)
def get_token():
    code = request.args.get('code')
    if not code:
        return "No authorization code provided."

    token_response = public_client.acquire_token_by_authorization_code(code=code,
                                                                       scopes=SCOPE,
                                                                       redirect_uri=AZURE_REDIRECT_URI)

    if 'access_token' in token_response:
        access_token = token_response['access_token']
        refresh_token = token_response['refresh_token']
        expires_in = token_response['expires_in']  # Token expiration time in seconds
        granted_at = int(time.time())  # Current time in seconds since epoch

        # Fetch user's first and last name using the access token
        user_info = requests.get(
            'https://graph.microsoft.com/v1.0/me',
            headers={'Authorization': 'Bearer ' + access_token}
        ).json()
        
        first_name = user_info.get('givenName', 'Unknown')
        last_name = user_info.get('surname', 'User')
        user_id = user_info.get('id')
        
        # Save tokens to a JSON file
        credentials = {'access_token': access_token,
                       'refresh_token': refresh_token,
                       'expires_in': expires_in,
                       'granted_at': granted_at,
                       'user_info': user_info
                       }
        profile_data = {'onedrive': user_id}
        
        save_token(AZURE_TOKENS_FOLDER, user_id, credentials)
        update_profile(PROFILES_FOLDER, session['profile_email'], profile_data)

        return f"Thank you {first_name} {last_name}!"

    return "Failed to obtain access token or refresh token."