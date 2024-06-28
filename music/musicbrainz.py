''' Streaming music sources and libraries '''

from sqlite3 import Row
import requests

from common.structure import MUSICBRAINZ_URL
from library.wordbank import RemoveWords
from music.dsp import Service
     
class MusicBrainer(Service):
    name = 'MusicBrainz'
    
    url = MUSICBRAINZ_URL
    data = {'fmt': 'json'}
    api_rate_limit = 1 # calls per second

    rid_limit = 100
    
    album_types = ['soundtrack', 'compilation', 'album', 'ep', 'live', 'remix']
    non_album_types = ['single']
    
    remove_words = [{'position': 'start',
                     'words': ['Remastered', 'Spotify Exclusive', '.*Anniversary Edition', 'Special.*Edition']},
                    {'position': 'end',
                     'words': ['Deluxe', 'Deluxe Edition', 'Deluxe Version', 'Remaster', 'Remastered', 'Standard Edition']}]
                    
    def __init__(self):
        super().__init__()
       
    def call_mb(self, term, query):
        try:
            response = requests.get(f'{self.url}/{term}', params=dict({'query': query}, **self.data), timeout=10)
            self.sleep()
            if response.ok:
                return response
            else:
                self.display_status(response)
                return self.no_response()
            
        except requests.exceptions.RequestException as e:
            print(f'Request failed: {e}')
            return self.no_response()
        
    def get_recordings_data(self, tracks_df):
        print('getting recordings data')
        tracks_df = self.get_recordings_info(tracks_df['isrc'].to_list())
        
        return tracks_df

    def get_recordings_info(self, isrcs):
        total_rows = len(isrcs)
        info_isrc = {}
        for i, isrc in enumerate(isrcs):
            self.show_progress(i, total_rows, message=f'ISRC: {isrc}')
            
            info__isrc = {}
            info__isrc['isrc'] = [isrc]
            info__isrc['iswc'] = [self.get_iswc(isrc)]
            
            info_isrc = self.combine_infos(info_isrc, info__isrc)
                
        self.show_progress()
        tracks_df = self.get_df_from_info(info_isrc)
        
        return tracks_df
    
    def get_works_data(self, tracks_df):
        print('getting works data')
        tracks_df = self.get_works_info(tracks_df['iswc'].to_list())
        
        return tracks_df
    
    def get_works_info(self, iswcs):
        total_rows = len(iswcs)
        info_iswc = {}
        for i, iswc in enumerate(iswcs):
            self.show_progress(i, total_rows, message=f'ISWC: {iswc}')
            
            info__iswc = {}
            info__iswc['iswc'] = [iswc]
            info__iswc['release_year'] = [self.get_first_release_year(iswc)]
            
            info_iswc = self.combine_infos(info_iswc, info__iswc)
                
        self.show_progress()
        tracks_df = self.get_df_from_info(info_iswc)
        
        return tracks_df
       
    def get_iswc(self, isrc):
        iswc = None

        response = self.call_mb('recording', f'isrc:{isrc}')
        if response.ok:
            recordings = response.json()['recordings']
            if len(recordings):
                rid = response.json()['recordings'][0]['id']
                response = self.call_mb('work', f'rid:{rid}')                
                if response.ok:
                    works = response.json()['works']
                    if len(works):
                        wid = response.json()['works'][0]['id']
                        response = self.call_mb('work', f'wid:{wid}')
                        if response.ok:
                            iswc = response.json()['works'][0].get('iswcs', [None])[0]

        return iswc

    def get_release_years(self, iswc):
        release_years = []
        response = self.call_mb('work', f'iswc:{iswc}')
        if response.ok:
            rids = [w['recording']['id'] for w in response.json()['works'][0]['relations'] if w['type'] == 'performance']
            rid_ors = (' OR ').join(f'"{r}"' for r in rids[0:min(self.rid_limit, len(rids))]) # stay within uri limit
            response = self.call_mb('recording', f'rid:({rid_ors})')
            if response.ok:
                release_years = [int(r.get('first-release-date')[0:4]) for r in response.json()['recordings'] \
                                    if r.get('first-release-date')]

        return release_years
    
    def get_first_release_year(self, iswc):
        release_years = self.get_release_years(iswc)
        first_release_year = min(release_years) if release_years else None
    
        return first_release_year 

    def get_mb_release_combos(self, album_s):
        upc_combos = [{'upc': album_s.get('upc')}]
        artist_album_combos = [{f'{a}_name': s, 'album_name': album_s['album_name']} for a in ['artist', 'alias'] for s in album_s['artist_names'].split('; ')] # artist/alias name and album title
        unknown_artist_album_combos = [{'artist_name': '[unknown artist]', 'album_name': album_s['album_name']}] # unknown artist and album title
        artist_date_combos = [{f'{a}_name': s, f'release_{r}': album_s['release_date']} for r in ['date', 'year'] for a in ['artist', 'alias'] for s in album_s['artist_names'].split('; ')] # artist/alias name and release date/year
        album_date_combos = [{'album_name': album_s['album_name'], f'release_{r}': album_s['release_date']} for r in ['date', 'year']] # album title and release date/year
        
        combos = upc_combos + artist_album_combos + unknown_artist_album_combos + artist_date_combos + album_date_combos
        return combos

    def find_barcodes_data(self, albums_df):
        self.announce('finding barcodes data')
        
        info_albums = {}
        total_rows = len(albums_df)
        for i, album_s in albums_df.iterrows():
            self.show_progress(i, total_rows, message=f'{album_s["album_name"]}')
            
            upc = None
            for combo in self.get_mb_release_combos(album_s):
                upc = self.get_upc(**combo)
                if upc:
                    break

            info__albums = {}
            info__albums['source_id'] = [album_s['source_id']]
            info__albums['album_uri'] = [album_s['album_uri']]
            info__albums['upc'] = [upc]
            info_albums = self.combine_infos(info_albums, info__albums)
        
        self.show_progress()
        albums_df = self.get_df_from_info(info_albums)
        
        return albums_df
        
    def get_barcodes_data(self, albums_df):
        self.announce('getting barcodes data')
        
        total_rows = len(albums_df)
        info_albums = {}
        for i, album_s in albums_df.iterrows():
            self.show_progress(i, total_rows, message=f'UPC: {album_s["upc"]}')
        
            release_type = None
            for combo in self.get_mb_release_combos(album_s):
                release_type = self.get_release_type(**combo)
                if release_type:
                    break

            info__albums = {}
            info__albums['upc'] = [album_s['upc']]
            info__albums['release_type'] = [release_type]
            info_albums = self.combine_infos(info_albums, info__albums)
            
        self.show_progress()
        albums_df = self.get_df_from_info(info_albums)
            
        return albums_df
    
    def get_release_type(self, upc=None, artist_name=None, alias_name=None, album_name=None, release_date=None, release_year=None):
        response = self.get_release_info(upc=upc, artist_name=artist_name, alias_name=alias_name, album_name=album_name,
                                         release_date=release_date, release_year=release_year)
        release_types = []
        if response.ok:
            releases = response.json()['releases']
            if len(releases):
                release_groups = releases[0]['release-group']
                release_types = []
                if 'secondary-types' in release_groups:
                    release_types += [rt.lower() for rt in release_groups['secondary-types']]
                elif 'primary-type' in release_groups:
                    release_types += [release_groups['primary-type'].lower()]
                
        release_type = next((a for a in self.album_types if a in release_types), None)
        self.add_text(f'release types: {release_types}')

        return release_type
            
    def get_upc(self, upc=None, artist_name=None, alias_name=None, album_name=None, release_date=None, release_year=None):
        response = self.get_release_info(artist_name=artist_name, alias_name=alias_name, album_name=album_name,
                                         release_date=release_date, release_year=release_year)
        if response.ok:
            releases = response.json()['releases']
            if len(releases):
                upc = releases[0].get('barcode')        
        
        return upc

    def get_release_info(self, upc=None, artist_name=None, alias_name=None, album_name=None, release_date=None, release_year=None):
        query = ''
        
        if upc:
            # barcode
            query = f'barcode:"{upc}"'

        else:    
            # combination of artist, title and release date   
            queries = []
            if alias_name:
                aliases = self.get_aliases(alias_name)
                if aliases:
                    artist_name = aliases[0]
            
            if artist_name:
                queries.append(f'artist:"{artist_name}"')
            if album_name:
                title, _ = self.remove_parentheticals(album_name.replace(" - ", " ").replace("-", ""), RemoveWords.albums)
                queries.append(f'release:"{title}"')
            if release_date:
                queries.append(f'date:{release_date.strftime("%Y-%m-%d")}')
            elif release_year:
                queries.append(f'date:{release_year.strftime("%Y")}')
                
            if len(queries) >= 2:
                # enough information to search
                query = ' AND '.join(queries)
            
        if query:
            not_queries = ' OR '.join(f'"{t}"' for t in self.non_album_types)
            query += f' AND NOT primarytype:({not_queries})'
            release_types = []
        
            response = self.call_mb('release', f'{query}')
           
        else:
            response = self.no_response()
            
        return response
        
    def get_aliases(self, artist_name):
        aliases = []
        response = self.call_mb('artist', f'alias:"{artist_name}"')
        if response.ok:
            aliases = [artist['name'] for artist in response.json()['artists'] if artist['name'] != artist_name]
            
        return aliases
    
