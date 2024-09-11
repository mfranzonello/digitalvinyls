import json
import os

from flask import Flask, render_template, request, redirect, url_for, session

from .auths.authorize_azure import azure_auth
from .auths.authorize_youtube import youtube_auth
from .auths.authorize_spotify import spotify_auth
from .auths.authorize_soundcloud import soundcloud_auth
from .auths.authorize_sonos import sonos_auth

from .common.structure import PROFILES_FOLDER
from .common.tokens import read_json, write_json
from .data.database import Neon
from .music.services import MUSIC_SERVICES

app = Flask(__name__)
app.secret_key = 'your_secret_key'

neon = Neon()
neon.connect()

def load_users():
    return read_json(PROFILES_FOLDER, 'users')
    
def load_profiles():
    return read_json(PROFILES_FOLDER, 'profiles')

def get_profile_services():
    return {profile['email']: [key for key in profile if key != 'email'] for profile in load_profiles()}
       
def save_users(users):
    write_json(PROFILES_FOLDER, 'users', users)

@app.route('/')
def index():
    if 'user' in session:
        message = f'Hello, {session["user"]["first_name"]} {session["user"]["last_name"]}!'
    else:
        message = 'Please sign in'

    return render_template('index.html', message=message)

@app.route('/login', methods=['GET', 'POST'])
def login():
    match request.method:
        case 'GET':
            return render_template('login.html')
        
        case 'POST':
            email = request.form.get('email')
            password = request.form.get('password')
            users = load_users()
            for user in users:
                if user['email'] == email and user['password'] == password:
                    session['user'] = user
                    return redirect(url_for('index'))
            return 'Invalid credentials', 401
   

@app.route('/register', methods=['GET', 'POST'])
def register():
    match request.method:
        case 'GET':
            return render_template('register.html')
        
        case 'POST':            
            email = request.form.get('email')
            password = request.form.get('password')
            first_name = request.form.get('first_name')
            last_name = request.form.get('last_name')
            users = load_users()
            if any(user['email'] == email for user in users):
                return 'User already exists', 400
            users.append({'email': email,
                          'password': password,
                          'first_name': first_name,
                          'last_name': last_name,
                          'profiles': [],
                          })
            save_users(users)
            session['user'] = {"first_name": first_name, "last_name": last_name, "email": email, "profiles": []}

            return redirect(url_for('index'))
    

@app.route('/profiles', methods=["GET", "POST"])
def profiles():
    if 'user' not in session:
        return redirect(url_for('login'))
    
    match request.method:
        case 'GET':
            return render_template('profiles.html',
                                   user_profiles=session['user']['profiles'],
                                   profile_services=get_profile_services(),
                                   music_services=MUSIC_SERVICES)
        
        case 'POST':
            first_name = request.form.get("first_name")
            last_name = request.form.get("last_name")
            email = request.form.get("email")
            users = load_users()
        
            for user in users:
                if user['email'] == session['user']['email']:
                    if any(profile['email'] == email for profile in user['profiles']):
                        return "Profile with this email already exists", 400
                    user['profiles'].append({"first_name": first_name, "last_name": last_name, "email": email})
                    save_users(users)
                    session['user'] = user  # Update session with the new profile
                    break
            return redirect(url_for('profiles'))

    
@app.route('/profiles/delete/<email>', methods=["POST"])
def delete_profile(email):
    if 'user' not in session:
        return redirect(url_for('login'))
    users = load_users()
    for user in users:
        if user['email'] == session['user']['email']:
            user['profiles'] = [profile for profile in user['profiles'] if profile['email'] != email or profile['email'] == user['email']]
            save_users(users)
            session['user'] = user  # Update session after deletion
            break
    return redirect(url_for('profiles'))


@app.route('/logout')
def logout():
    session.pop('user', None)
    return redirect(url_for('index'))


@app.route('/play')
def play():
    if 'user' in session:
        message = f'Select what music you want to play.'
        dataframe = [10]
    else:
        return redirect(url_for('index'))
    return render_template('play.html', message=message, dataframe=dataframe)


@app.route('/rank_albums', methods=['GET', 'POST'])
def rank_albums():
    match request.method:
        case 'GET':
            # Get two random albums with the same category
            albums_df = neon.get_album_comparisons(1)
            album_1 = albums_df.iloc[0]
            album_2 = albums_df.iloc[1]
            category = albums_df.loc[:, 'category'].iloc[0]
            
            return render_template('rank_albums.html', category=category, album1=album_1, album2=album_2)
        
        case 'POST':
            selected_album = request.form['selected_album']
            # Update your database with the selected album here
            print(f'User selected album: {selected_album}')
            return redirect(url_for('rank_albums'))
        
# # @app.route('/top_albums')
# # def top_albums():
# #     albums_df = neon.get_user_albums(1)
# #     return render_template('top_albums.html')
    
    
# register Blueprints
app.register_blueprint(azure_auth, url_prefix='/authorize/azure')
app.register_blueprint(youtube_auth, url_prefix='/authorize/youtube')
app.register_blueprint(spotify_auth, url_prefix='/authorize/spotify')
app.register_blueprint(soundcloud_auth, url_prefix='/authorize/soundcloud')
app.register_blueprint(sonos_auth, url_prefix='/authorize/sonos')

if __name__ == '__main__':
    print(os.getcwd())
    app.run(debug=True, port=5000)
