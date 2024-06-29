''' Authenticate a user to access OneDrive '''

import requests
import json
import os
import sys
import time

import requests
from flask import Flask, request, redirect
import msal

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

# Add the parent directory to sys.path
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Now you can import the module
from common.secret import get_secret
from common.structure import write_json
from common.structure.VALUES import AZURE_REDIRECT_URI, AZURE_TENANT_ID, AZURE_SCOPE, AZURE_TOKENS_FOLDER

# Azure AD app registration configuration
CLIENT_ID = get_secret('AZURE_CLIENT_ID')
CLIENT_SECRET = get_secret('AZURE_CLIENT_SECRET')
TENANT_ID = AZURE_TENANT_ID
SCOPE = AZURE_SCOPE

# Redirect URI for the application (must be registered in Azure AD)
REDIRECT_PATH = '/getAToken' #AZURE_REDIRECT_URI

# Azure AD app registration configuration
AUTHORITY = f'https://login.microsoftonline.com/common' #{TENANT_ID}'
REDIRECT_URI = f'http://localhost:5000{REDIRECT_PATH}'

# Flask app setup
app = Flask(__name__)
app.secret_key = os.urandom(24)  # Use a secure random key

# Create the MSAL public client application
public_client = msal.PublicClientApplication(
    client_id=CLIENT_ID,
    authority=AUTHORITY
)

@app.route('/')
def index():
    # Construct the authorization request URL
    auth_url = public_client.get_authorization_request_url(
        scopes=SCOPE,
        redirect_uri=REDIRECT_URI
    )
    return redirect(auth_url)

@app.route(REDIRECT_PATH)
def get_token():
    code = request.args.get('code')
    if not code:
        return "No authorization code provided."

    token_response = public_client.acquire_token_by_authorization_code(
        code=code,
        scopes=SCOPE,
        redirect_uri=REDIRECT_URI
    )

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
        write_json(AZURE_TOKENS_FOLDER, user_id, credentials)

        return f"Thank you {first_name} {last_name}!"

    return "Failed to obtain access token or refresh token."

if __name__ == '__main__':
    print("Please visit http://localhost:5000 to authenticate.")
    app.run(host='127.0.0.1', port=5000)