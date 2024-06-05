import requests
import base64

from common.secret import get_secret


scopes = ["playlist-modify-public", "ugc-image-upload", "user-read-private", "user-read-email", "user-library-read"]

CLIENT_ID = get_secret('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = get_secret('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = 'http://localhost:8000/callback/'

# URLS
AUTH_URL = 'https://accounts.spotify.com/authorize'
TOKEN_URL = 'https://accounts.spotify.com/api/token'
BASE_URL = 'https://api.spotify.com/v1/'


# Make a request to the /authorize endpoint to get an authorization code
auth_code = requests.get(AUTH_URL, {
    'client_id': CLIENT_ID,
    'response_type': 'code',
    'redirect_uri': 'https://open.spotify.com/collection/playlists',
    'scope': ' '.join(scopes),
})
print(auth_code)

auth_header = base64.urlsafe_b64encode((CLIENT_ID + ':' + CLIENT_SECRET).encode())

headers = {
    'Content-Type': 'application/x-www-form-urlencoded',
    'Authorization': 'Basic %s' % auth_header.decode('ascii')
}

payload = {
    'grant_type': 'authorization_code',
    'code': auth_code,
    'redirect_uri': REDIRECT_URI,
    #'client_id': CLIENT_ID,
    #'client_secret': CLIENT_SECRET,
}

# Make a request to the /token endpoint to get an access token
access_token_request = requests.post(url=TOKEN_URL, data=payload, headers=headers)

# convert the response to JSON
access_token_response_data = access_token_request.json()

print(access_token_response_data)

# save the access token
access_token = access_token_response_data['access_token']

print(access_token_response_data)