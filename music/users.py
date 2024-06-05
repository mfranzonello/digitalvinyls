class User:
    def __init__(self, user_id, first_name, last_name, spotify_user_id=None, soundcloud_username=None):
        self.user_id = user_id
        self.first_name = first_name
        self.last_name = last_name
        self.spotify_user_id = spotify_user_id
        self.soundcloud_username = soundcloud_username
        self.image_src = None
        
    def add_spotify(self, spotify_user_id):
        self.spotify_user_id = spotify_user_id
        
    def has_spotify(self):
        return self.spotify_user_id is not None
        
    def add_soundcloud(self, soundcloud_username):
        self.soundcloud_username = soundcloud_username

    def has_soundcloud(self):
        return self.soundcloud_username is not None

    def add_picture(self, image_src):
        self.image_src = image_src
