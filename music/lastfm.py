''' LastFM API functions - requires dev keys '''

from ..common.calling import Caller
from ..common.secret import get_secret
from ..common.structure import LAST_FM_SEARCH_URL, LAST_FM_RATE_LIMIT

class FMer(Caller):
    search_url = LAST_FM_SEARCH_URL # 'https://ws.audioscrobbler.com/2.0'
    api_rate_limit = LAST_FM_RATE_LIMIT
    
    def __init__(self):
        super().__init__()
        
    def get_params(self, params={}):
        params.update({'api_key': get_secret('LAST_FM_API_KEY'),
                       'format': 'json'})
        return params
    
    def get_headers(self):
        return {}

    # # def get_musicbrainz_info(self, mbid):
    # #     params = {'method': 'album.getinfo',
    # #               'mbid': mbid,
    # #               'album': 1}
    # #     _, response = self.invoke_api(f'{self.search_url}', method='get', params=self.get_params(params), headers=self.get_headers())
    # #     if response.ok:
    # #         items = response.json()
    # #         album = items.get('album')
    # #         return album
        
    def get_album_info(self, album_name, artist_name):
        params = {'method': 'album.getinfo',
                  'album': album_name,
                  'artist': artist_name}
        _, response = self.invoke_api(f'{self.search_url}', method='get', params=self.get_params(params), headers=self.get_headers())
        if response.ok:
            items = response.json()
            album = items.get('album')
            return album     
        
    def search_for_album(self, album_name, artist_names):
        params = {'method': 'album.search',
                  'album': album_name}
        _, response = self.invoke_api(f'{self.search_url}', method='get', params=self.get_params(params), headers=self.get_headers())
        if response.ok:
            items = response.json()
            albums = items['results']['albummatches']['album']
            if albums:
                matches = [album for album in albums if any(album['artist'].lower() == artist_name.lower() for artist_name in artist_names)]
            album_name = matches[0]['name']
            artist_name = matches[0]['artist']
            
            album = self.get_album_info(album_name, artist_name)
            return album
        
    def get_tags_from_album(self, album):
        tags = album['tags']
        if tags and tags.get('tag'):
            tags = tags['tags']
            genres = [tag['name'].lower() for tag in tags]
        else:
            genres = []
            
        return genres
        
    def check_words_in_genres(words, genres):
        return any(word in genre for word in words for genre in genres)

    def check_album_type(self, album):
        genres = self.get_tags_from_album(album)
        soundtrack_words = ['soundtrack']
        score_words = ['instrumental', 'orchestral', 'ambient', 'score']        
        compilation_words = ['compilation', 'greatest hits']
        if self.check_words_in_genres(soundtrack_words, genres):
            if self.check_words_in_genres(score_words, genres):
                release_type = 'score'
            else:
                release_type = 'soundtrack'
                
        elif self.check_words_in_genres(compilation_words, genres):
            release_type = 'compilation'
            
        else:
            release_type = None
            
    def get_release_type(self, album_name, artist_names): #, mbid=None):
        # # if mbid:
        # #     album = self.get_musicbrainz_info(mbid)
        # #     release_type = self.check_album_type(album)
        ##if not release_type or not mbid:
        album = self.search_for_album(album_name, artist_names)
        release_type = self.check_album_type(album)
            
        return release_type