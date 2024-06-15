from datetime import datetime
import json

from pandas import read_sql, isna, DataFrame, Series
from sqlalchemy import create_engine, text

from common.secret import get_secret
from common.structure import NEON_DB_NAME, NEON_USERNAME, NEON_HOST

class Neon:
    tables = [{'name': 'services',
               'columns': [['service_id', 'serial'],
                           ['service_name', 'varchar'],
                           ],
               'pk': ['service_id'],
               'unique': ['service_name', 'service_source'],
               },
              {'name': 'sources',
               'columns': [['source_id', 'serial'],
                           ['service_id', 'int'],
                           ['source_name', 'varchar'],
                           ],
               'pk': ['source_id'],
               'fk': [[['service_id'], 'services'],
                      ],               
               'unique': ['service_id', 'service_source'],
               }, 
              {'name': 'users',
               'columns': [['user_id', 'serial'],
                           ['first_name', 'varchar'],
                           ['last_name',  'varchar'],
                           ['service_user_ids', 'jsonb'],
                           ['image_src', 'varchar'],
                           ],
               'pk': ['user_id'],
               },
              {'name': 'artists',
               'columns': [['service_id', 'integer'],
                           ['artist_id', 'varchar'],
                           ['artist_name', 'varchar'],
                           ],
               'pk': ['service_id', 'artist_id'],
               'fks': [[['service_id'], 'services'],
                       ],
               },
              {'name': 'albums',
               'columns': [['source_id', 'integer'],
                           ['album_id', 'varchar'],
                           ['artist_ids', 'jsonb'],
                           ['album_name', 'varchar'],
                           ['album_type', 'varchar'],
                           ['genres', 'jsonb'],
                           ['release_date', 'timestamp'],
                           ['image_src', 'varchar'],
                           ['track_list', 'jsonb'],
                           ['album_duration', 'numeric'],
                           ['upc', 'varchar'],
                           ],         
               'pk': ['source_id', 'album_id'],
               'fk': [[['source_id'], 'sources'],
                      ],
               },
              {'name': 'ownerships',
               'columns': [['user_id', 'integer'],
                           ['source_id', 'integer'],
                           ['album_id', 'varchar'],
                           ['like_date', 'timestamp'],
                           ['rating', 'integer'],
                           ['ranking', 'integer'],
                           ],
               'pk': ['user_id', 'source_id', 'album_id'],
               'fks': [[['user_id'], 'users'],
                       [['source_id', 'album_id'], 'albums'],
                       ],
               },
              {'name': 'tracks',
               'columns': [['service_id', 'integer'],
                           ['track_id', 'varchar'],
                           ['track_name', 'varchar'],
                           ['track_duration', 'numeric'],
                           ['artist_ids', 'jsonb'],
                           ['isrc', 'varchar'],
                           ['release_year', 'integer'],
                           ],
               'pk': ['service_id', 'track_id'],
               'fks': [[['service_id'], 'services'],
                       ],
               },
              {'name': 'series',
               'columns': [['user_id', 'integer'],
                           ['series_name', 'varchar'],
                           ['album_list', 'jsonb'], # --> [[source_id, [album_ids]], [...]]
                           ],
               'pk': ['user_id', 'series_name'],
               'fk': [[['user_id'], 'users']]
               },
              {'name': 'skips',
               'columns': [['user_id', 'integer'],
                           ['source_id', 'integer'],
                           ['album_id', 'varchar'],
                           ['skip_list', 'jsonb'],
                           ],
               'pk': ['user_id', 'source_id'],
               'fks': [[['user_id', 'source_id', 'album_id'], 'ownerships'],
                       ],
               },
              {'name': 'replacements',
               'columns': [['source_id', 'integer'],
                           ['album_id', 'varchar'],
                           ['replace_source_id', 'integer'],
                           ['replace_album_id', 'varchar'],
                           ],
               'pk': ['source_id', 'album_id'],
               'fks': [[['source_id', 'album_id'], 'albums'],
                       [{'replacement_source_id': 'source_id', 'replacement_album_id': 'album_id'}, 'albums'],
                       ],
               },
              {'name': 'barcodes',
               'columns': [['barcode_id', 'series'],
                           ['upc', 'varchar'],
                           ['release_type', 'varchar'],
                           ],
               'pk': ['barcode_id'],
               'uniques': ['upc'],
               },
              ]
  
    views = ['album_categories', 'compilations', 'user_albums',
             'update_compilations', 'update_tracks', 'update_albums', 'update_soundtracks', 'update_barcodes']

    def __init__(self):
        self.connection = None
    
    def connect(self):
        username = NEON_USERNAME
        password = get_secret('NEON_PASSWORD')
        host = NEON_HOST
        db_name = NEON_DB_NAME
        url = f'postgresql://{username}:{password}.{host}/{db_name}?sslmode=require'
        engine = create_engine(url, pool_pre_ping=True)
        self.connection = engine.connect()
        
    ''' basic DB functions '''
    def dbify(self, value):
        if isinstance(value, str):
            value = "'" + value.replace("'", "''") + "'"
            
        elif isinstance(value, datetime):
            value = "'" + value.strftime('%Y-%m-%d') + "'::date"
            
        elif isinstance(value, (list, dict)):
            value = "'" + json.dumps(value) + "'::jsonb"
            
        elif (value is None) or isna(value):
            value = 'NULL'

        return str(value)
    
    def regify(self, values):
        if isinstance(values, (int, str, float)):
            values = [str(values)]
            
        return '%%(' + '|'.join(self.dbify(v.lower()) for v in values) + ')%%'
    
    def execute(self, sql=None, commit=True):
        if sql:
            self.connection.execute(text(sql))
        if commit:
            self.connection.commit()
            
    def read_sql(self, sql):
        return read_sql(sql, self.connection)
    
    def create_primary_key(self, pk):
        key = 'PRIMARY KEY (' + ', '.join(pk) + ')' if pk else ''
        
        return key
    
    def create_foreign_key(self, fkt):
        if fkt:
            fk, t = fkt
            if isinstance(fk, list):
                fk1 = fk
                fk2 = fk
            elif isinstance(fk, dict):
                fk1 = fk.keys()
                fk2 = fk.values()
                
            key = 'FOREIGN KEY (' + ', '.join(fk1) + f') REFERENCES {t} (' + ', '.join(fk2) + ')' 

        return key

    ''' table setup '''
    def create_tables(self):
        for table in self.tables:
            cols_add = ', '.join(f'{c} {t}' for c, t in table['columns'])
            pk_add = ', ' + self.create_primary_key(table['pk']) if table['pk'] else ''
            fk_add = ', ' + ', '.join(self.create_foreign_key([fk, t]) for fk, t in table['fks']) if table.get('fks') else ''
            sql = (f'CREATE TABLE IF NOT EXISTS {table["name"]} '
                   f'({cols_add} {pk_add} {fk_add})'
                   f';'
                   )
            self.execute(sql, commit=False)

        self.execute()
            
    def drop_tables(self):
        for table in self.tables.keys()[::-1]:
            sql = f'DROP TABLE IF EXISTS {table["name"]};'
            self.execute(sql, commit=False)

        self.execute()

    ''' view setup '''                    
    def create_views(self):
        for view_sql in [self.create_compilations_view,
                         self.create_categories_view,
                         self.create_user_albums_view,
                         self.create_update_compilations_view,
                         self.create_update_tracks_view, 
                         self.create_update_artists_view, 
                         self.create_update_soundtracks_view,
                         ]:
            view_sql()

    def drop_views(self):
        for view_name in self.views[::-1]:
            print(view_name)
            sql = f'DROP VIEW IF EXISTS {view_name} CASCADE;'
            self.execute(sql, commit=False)
        self.execute(commit=True)

    def create_view(self, view_name, view_sql):
        sql = f'CREATE OR REPLACE VIEW {view_name} AS {view_sql}'
        self.execute(sql)
        
    def create_update_view(self, view_name, view_sql):
        self.create_view(f'update_{view_name}', view_sql)

    def create_compilations_view(self):
        sql = (f'SELECT albums_expanded.source_id, albums_expanded.album_id, '
               f'jsonb_agg(DISTINCT COALESCE(tracks.release_year, EXTRACT(YEAR FROM albums_expanded.release_date))) AS release_years '
               f'FROM (SELECT albums.source_id, albums.album_id, albums.release_date, arr.track_id '
               f'FROM albums, jsonb_array_elements_text(track_list) as arr(track_id)) AS albums_expanded '
               f'JOIN sources ON albums_expanded.source_id = sources.source_id '
               f'LEFT JOIN tracks ON sources.service_id = tracks.service_id '
               f'AND albums_expanded.track_id = tracks.track_id '
               f'GROUP BY albums_expanded.source_id, albums_expanded.album_id '
               f';'
               )
        self.create_view('compilations', sql)
    
    def create_update_compilations_view(self):
        sql = (f'SELECT tracks.isrc FROM albums JOIN sources ON albums.source_id = sources.source_id '
               f'JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_list ? tracks.track_id '
               f'WHERE albums.album_type = {self.dbify("compilation")} '
               f'AND tracks.release_year IS NULL AND tracks.isrc IS NOT NULL '
               f';'
               )
        self.create_update_view('compilations', sql)
    
    def create_update_tracks_view(self):
        sql = (f'SELECT sources.service_id, jsonb_array_elements_text(albums.track_list) AS track_id FROM albums '
               f'JOIN sources ON albums.source_id = sources.source_id '
               f'EXCEPT SELECT service_id, track_id FROM tracks WHERE track_name IS NOT NULL'
               f';'
               )
        self.create_update_view('tracks', sql)

    def create_update_artists_view(self):
        sql = (f'SELECT service_id, jsonb_array_elements_text(artist_ids) AS artist_id FROM tracks '
               f'UNION SELECT sources.service_id, jsonb_array_elements_text(albums.artist_ids) AS artist_id FROM albums '
               f'JOIN sources ON albums.source_id = sources.source_id '
               f'EXCEPT SELECT service_id, artist_id FROM artists WHERE artist_name IS NOT NULL '
               f';'
               )
        self.create_update_view('artists', sql)
    
    def create_categories_view(self):
        sql = (f'WITH regex AS (SELECT {self.dbify("%%(")} || string_agg(phrase, {self.dbify("|")}) || {self.dbify(")%%")} AS phrases '
               f'FROM keywords WHERE keyword = {self.dbify("soundtrack")}), '
               
               f'composer_artists AS (SELECT array_agg(artist_id) AS composer_ids FROM artists '
               f'WHERE artist_name IN (SELECT artist_name FROM composers) GROUP BY service_id) '
               
               f'SELECT source_id, album_id, '
               f'(CASE WHEN (album_type = {self.dbify("single")} AND '
               f'jsonb_array_length(track_list) >= (select phrase from keywords where keyword = {self.dbify("min_ep_tracks")})::numeric) THEN {self.dbify("ep")} '
               f'WHEN album_type in ({self.dbify("album")}, {self.dbify("compilation")}) THEN '
               f'(CASE WHEN lower(album_name) SIMILAR TO regex.phrases OR artist_ids ?| (SELECT composer_ids FROM composer_artists) THEN {self.dbify("soundtrack")} '
               f'WHEN album_type = {self.dbify("album")} THEN {self.dbify("studio")} ELSE album_type END) ELSE album_type END) AS category '
               f'FROM albums, regex '
               f';'
               )
        self.create_view('album_categories', sql)
    
    def create_update_soundtracks_view(self):
        sql = (f'SELECT tracks.service_id, tracks.track_id FROM tracks '
               f'JOIN sources ON tracks.service_id = sources.service_id '
               f'JOIN albums ON sources.source_id = albums.source_id AND albums.track_list ? tracks.track_id '
               f'JOIN album_categories ON albums.source_id = album_categories.source_id AND albums.album_id = album_categories.album_id '
               f'WHERE category = {self.dbify("soundtrack")} AND instrumentalness IS NULL '
               f';'
               )
        self.create_update_view('soundtracks', sql)

    def create_user_albums_view(self):
        sql = (f'WITH album_artists AS (SELECT albums_expanded.source_id, albums_expanded.album_id, string_agg(artists.artist_name, {self.dbify("; ")} '
               f'ORDER BY albums_expanded.ord) AS artist_names FROM (SELECT albums.source_id, albums.album_id, arr.artist_id, arr.ord '
               f'FROM albums, jsonb_array_elements_text(artist_ids) WITH ORDINALITY arr(artist_id, ord)) AS albums_expanded '
               f'JOIN sources ON albums_expanded.source_id = sources.source_id '
               f'JOIN artists ON sources.service_id = artists.service_id AND albums_expanded.artist_id = artists.artist_id '
               f'GROUP BY albums_expanded.source_id, albums_expanded.album_id), '
               
               f'instrumentals AS (SELECT albums.source_id, albums.album_id, avg(tracks.instrumentalness) as instrumentalness '
               f'FROM albums JOIN sources ON albums.source_id = sources.source_id '
               f'JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_list ? tracks.track_id '
               f'GROUP BY albums.source_id, albums.album_id) '
               
               f'SELECT album_artists.artist_names, albums.album_name,  albums.release_date, '
               f'(CASE WHEN album_categories.category = {self.dbify("soundtrack")} AND '
               f'COALESCE(instrumentals.instrumentalness, 0) > (SELECT phrase FROM keywords WHERE keyword = {self.dbify("score_instrumental")})::numeric '
               f'THEN {self.dbify("score")} ELSE album_categories.category end) AS category, '
               f'albums.album_duration, compilations.release_years, '
               f'albums.source_id, albums.album_id, albums.artist_ids, albums.track_list, '
               f'users.user_id, users.first_name || {self.dbify(" ")} || users.last_name AS user_name, '
               f'ownerships.like_date, ownerships.rating, ownerships.ranking FROM albums '
               f'JOIN album_artists ON albums.source_id = album_artists.source_id AND albums.album_id = album_artists.album_id '
               f'JOIN sources ON albums.source_id = sources.source_id '
               f'JOIN album_categories ON albums.source_id = album_categories.source_id AND albums.album_id = album_categories.album_id '
               f'JOIN compilations ON albums.source_id = compilations.source_id AND albums.album_id = compilations.album_id '
               f'JOIN ownerships ON ownerships.source_id = albums.source_id AND ownerships.album_id = albums.album_id '
               f'JOIN users ON users.user_id = ownerships.user_id '
               f'LEFT JOIN instrumentals ON sources.source_id = instrumentals.source_id AND albums.album_id = instrumentals.album_id '
               f'ORDER BY albums.release_date DESC '
               f';'
               )
        self.create_view('user_albums', sql)
        
    def create_update_barcodes_view(self):
        sql = (f'SELECT upc FROM albums WHERE album_type IN ({self.dbify("album")}, {self.dbify("compilation")}) '
               f'EXCEPT SELECT upc FROM barcodes WHERE release_type IS NOT NULL;'
               f';'
               )
        self.create_updates_view('barcode', sql)

    ''' update data from pull '''
    def update_service_table(self, df, table_name, columns, pk_columns, service_id=None, source_id=None, update_only=False):
        s_id = ['service_id'] if service_id else ['source_id'] if source_id else []
        s_id_v = f'{self.dbify(service_id)}, ' if service_id else f'{self.dbify(source_id)}, ' if source_id else ''
        values = ', '.join('(' + s_id_v + ', '.join(self.dbify(v) for v in r) + ')' for r in df[columns].values)
            
        if not update_only:
            # upserting
            insert_columns = ', '.join(s_id + columns)
            conflict_columns = ', '.join(s_id + pk_columns)
            excludes = ', '.join(f'{c} = EXCLUDED.{c}' for c in columns if c not in pk_columns)
            sql = (f'INSERT INTO {table_name} ({insert_columns}) VALUES {values} '
                   f'ON CONFLICT ({conflict_columns}) DO UPDATE SET {excludes} '
                   f';'
                   )
        else:
            # updating
            sets = ', '.join(f'{c} = updt.{c}' for c in columns if c not in s_id + pk_columns)
            ases = ', '.join(s_id + columns)
            wheres = ' AND '.join(f'updt.{c} = {table_name}.{c}' for c in s_id + pk_columns)
            sql = (f'UPDATE {table_name} SET {sets} FROM (VALUES {values}) '
                   f'AS updt({ases}) WHERE {wheres} '
                   f';'
                   )
            
        self.execute(sql)
        
    def update_artists(self, artists_df, service_id):
        columns = ['artist_id', 'artist_name']
        self.update_service_table(artists_df, 'artists', columns, ['artist_id'], service_id=service_id)
        
    def update_albums(self, albums_df, source_id):
        columns = ['album_id', 'artist_ids', 'album_name', 'album_type', 
                   'release_date', 'image_src', 'album_duration', #'genres',
                   'track_list', 'replacement', 'upc'] #skip_list
        self.update_service_table(albums_df, 'albums', columns, ['album_id'], source_id=source_id)
        
    def update_ownerships(self, ownerships_df, source_id, user_id):
        columns = ['user_id', 'album_id', 'like_date'] #, 'rating', 'ranking']
        ownerships_df['user_id'] = user_id
        self.update_service_table(ownerships_df, 'ownerships', columns, ['user_id', 'album_id'], source_id=source_id)
        
    def update_tracks(self, tracks_df, service_id):
        columns = ['track_id', 'track_name', 'track_duration', 'artist_ids', 'isrc']
        self.update_service_table(tracks_df, 'tracks', columns, ['track_id'], service_id=service_id)

    def update_soundtracks(self, tracks_df, service_id):
        columns = ['track_id', 'instrumentalness']
        self.update_service_table(tracks_df, 'tracks', columns, ['track_id'], service_id=service_id, update_only=True)

    def update_compilations(self, tracks_df):
        columns = ['isrc', 'release_year']
        self.update_service_table(tracks_df, 'tracks', columns, ['isrc'], update_only=True)
        
    def update_barcodes(self, albums_df):
        columns = ['upc', 'release_type']
        self.update_service_table(albums_df, 'barcodes', columns, ['upc'])

    def update_user(self, user):
        columns = ['user_id', 'first_name', 'last_name', 'service_user_ids', 'image_src']
        self.update_service_table(user, 'users', columns, ['user_id'])
        
    def update_series(self, user_id, series_name, artist_id=None, artist_name=None, album_name_pattern=None, album_not_pattern=None):
        artist_yes = f'artist_ids ? {self.dbify(artist_id)}' if artist_id else None
        artist_maybe = f'LOWER(artist_names) SIMILAR TO {self.regify(album_name_pattern)}' if artist_name else None
        album_yes = f'LOWER(album_names) SIMILAR TO {self.regify(album_name_pattern)}' if album_name_pattern else None
        album_no = f'LOWER(album_name) NOT SIMILAR TO {self.regify(album_not_pattern)}' if album_name_pattern else None
        
        sql = (f'WITH new_series AS (SELECT jsonb_agg(row_data) AS album_list '
               f'FROM (SELECT jsonb_build_array(source_id, album_id) AS row_data '
               f'FROM user_albums WHERE {" AND ".join(c for c in [artist_yes, artist_maybe, album_yes, album_no] if c is not None)} '
               f'ORDER BY release_date)) '

               f'INSERT INTO series (user_id, series_name, album_list) '
               f'VALUES ({user_id}, {self.dbify(series_name)}, (SELECT album_list FROM new_series)) '
               f';'
               )
        self.execute(sql)


    ''' manual changes '''
    def update_ranking(self, user_id, album_id, above_album_id=None):
        # check if album was already ranked
        sql = f'SELECT ranking FROM ownerships WHERE user_id = {user_id} AND album_id = {album_id};'
        ranking = self.read_sql(sql)
        
        # check what the highest current rank is
        sql = f'SELECT max(ranking) FROM ownerships WHERE user_id = {user_id};'
        max_ranking = self.read_sql(sql).squeeze()
        
        # get the ranking of the album already
        current_ranking = ranking.squeeze() if len(ranking) else max_ranking + 1
        
        # get the ranking of the album to swap with
        if above_album_id:
            sql = f'SELECT ranking FROM ownerships WHERE album_id = {self   .dbify(above_album_id)} AND user_id = {self.dbify(user_id)}'
            ranking = self.read_sql(sql)
            previous_ranking = ranking.squeeze() if len(ranking) else max_ranking
        else:
            sql = (f'SELECT max(ranking) + 1 FROM ownerships')
            previous_ranking = max_ranking
            
        # move other albums down
        sql = (f'UPDATE ownerships SET ranking = ranking + 1 WHERE ranking >= {previous_ranking} AND ranking < {current_ranking} '
               f'AND user_id = {self.dbify(user_id)}')
        self.execute(sql)
        # move the album up
        sql = f'UPDATE ownerships SET ranking = {previous_ranking} WHERE album_id = {self.dbify(album_id)} AND user_id = {self.dbify(user_id)};'
        self.execute(sql)
        
    def add_rating(self, user_id, album_id, rating):
        sql = f'UPDATE ownerships SET rankings = {rating} WHERE album_id = {self.dbify(album_id)} AND user_id = {self.dbify(user_id)}'
        self.execute(sql)        

    ''' get data for pull '''
    def get_service_id(self, service_name):
        sql = f'SELECT service_id FROM services WHERE service_name = {self.dbify(service_name)};'
        service_id = self.read_sql(sql).squeeze()
        return service_id        
    
    def get_source_id(self, service_id, source_name):
        sql = f'SELECT source_id FROM sources WHERE service_id = {self.dbify(service_id)} AND source_name = {self.dbify(source_name)};'
        source_id = self.read_sql(sql).squeeze()
        return source_id
    
    def get_sources(self, service_id):
        sql = f'SELECT source_id, source_name FROM sources WHERE service_id = {self.dbify(service_id)};'
        sources_df = self.read_sql(sql)
        return sources_df
      
    def get_table_to_update(self, table_name, service_id=None, source_id=None, limit=1000):# limit=None):
        limits = f'LIMIT {limit}' if limit else ''
        wheres = f'WHERE service_id = {service_id}' if service_id else f'WHERE source_id = {source_id}' if source_id else ''
        sql = (f'SELECT * FROM update_{table_name} {wheres} {limits};')
        updates_df = self.read_sql(sql)
        return updates_df
    
    def get_tracks_to_update(self, service_id):
        tracks_df = self.get_table_to_update('tracks', service_id=service_id)
        return tracks_df

    def get_artists_to_update(self, service_id):
        artists_df = self.get_table_to_update('artists', service_id=service_id)
        return artists_df

    def get_soundtracks_to_update(self, service_id):
        tracks_df = self.get_table_to_update('soundtracks', service_id=service_id)
        return tracks_df

    def get_compilations_to_update(self):
        tracks_df = self.get_table_to_update('compilations', limit=25)
        return tracks_df

    def get_barcodes_to_update(self):
        tracks_df = self.get_table_to_update('barcodes', limit=25)
        return tracks_df

    def get_user_ids(self):
        sql = (f'SELECT user_id FROM users;')
        user_ids = self.read_sql(sql)['user_id'].to_list()
        return user_ids

    def get_user(self, user_id):
        sql = (f'SELECT * FROM users '
               f'WHERE user_id = {self.dbify(user_id)};'
               )
        user_s = self.read_sql(sql).squeeze()
        return user_s
    

    ''' get data for push '''
    def get_user_albums(self, user):
        sql = f'SELECT * FROM user_albums WHERE user_id = {self.dbify(user.user_id)}'
        albums_df = self.read_sql(sql)
        return albums_df
    
    def get_random_album(self, user_id):
        sql = (f'SELECT service_id, album_id, artist_name, album_name, '
               f'track_list ' #', track_names '
               f'FROM user_albums WHERE user_id = {self.dbify(user_id)} '
               f'ORDER BY random() LIMIT 1'
               f';'
               )
        album_s = self.read_sql(sql).squeeze()
        return album_s