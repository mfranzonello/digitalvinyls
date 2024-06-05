from datetime import datetime
import json

from pandas import read_sql
from sqlalchemy import create_engine, text

from common.secret import get_secret

class Neon:
    def __init__(self):
        self.connection = None
    
    def connect(self):
        username = 'digitalvinyl_owner'
        password = 'IrpF3U7DTdyK@ep-odd-darkness-a42knqc3'
        host = 'us-east-1.aws.neon.tech'
        db_name = 'digitalvinyl'
        url = f'postgresql://{username}:{password}.{host}/{db_name}?sslmode=require'
        engine = create_engine(url)
        self.connection = engine.connect()
        
    def dbify(self, value):
        if isinstance(value, str):
            value = "'" + value.replace("'", "''") + "'"
            
        elif isinstance(value, datetime):
            value = "'" + value.strftime('%Y-%m-%d') + "'::date"
            
        elif isinstance(value, (list, dict)):
            value = "'" + json.dumps(value) + "'::jsonb"
            
        elif value is None:
            value = 'NULL'

        return str(value)
    
    def execute(self, sql=None, commit=True):
        if sql:
            self.connection.execute(text(sql))
        if commit:
            self.connection.commit()
            
    def read_sql(self, sql):
        return read_sql(sql, self.connection)

    def create_tables(self):
        for table_sql in [self.create_services_table, self.create_users_table,
                          self.create_artists_table, self.create_albums_table,
                          #self.create_tracks_table, self.create_series_table
                          self.create_likes_table]:
            sql = table_sql()
            self.execute(sql, commit=False)
            
        self.execute()

    
    def drop_tables(self):
        for table_name in ['Likes', 'Albums', 'Artists', 'Users', 'Services']:
            sql = f'DROP TABLE IF EXISTS {table_name};'
            self.execute(sql, commit=False)

        self.execute()

    def create_views(self):
        for view_sql in [self.create_user_albums_view]:
            sql = view_sql()
            self.execute(sql, commit=False)
            
        self.execute()
        
    def drop_views(self):
        pass

    def create_services_table(self):
        sql = (f'CREATE TABLE IF NOT EXISTS Services '
               f'(service_id serial, service_name varchar, '
               f'PRIMARY KEY (service_id) '
               f');'
               )
        return sql
        
    def create_users_table(self):
        sql = (f'CREATE TABLE IF NOT EXISTS Users '
               f'(user_id serial, first_name varchar, last_name varchar, '
               f'service_user_ids jsonb, image_src varchar, '
               f'PRIMARY KEY (user_id) '
               f');'
               )
        return sql
                
    def create_artists_table(self):
        sql = (f'CREATE TABLE IF NOT EXISTS Artists '
               f'(service_id integer, artist_id varchar, artist_name varchar, '
               f'PRIMARY KEY (service_id, artist_id), '
               f'FOREIGN KEY (service_id) REFERENCES Services (service_id) '
               f');'
               )
        return sql
    
    def create_albums_table(self):
        sql = (f'CREATE TABLE IF NOT EXISTS Albums '
               f'(service_id integer, album_id varchar, '
               f'artist_id varchar, album_name varchar, album_type varchar, ' # should artist_id be jsonb?
               f'genres jsonb, categorization varchar, '
               f'release_date date, image_src varchar, '
               f'track_list jsonb, skip_list jsonb, replacement jsonb, '
               f'PRIMARY KEY (service_id, album_id), '
               f'FOREIGN KEY (service_id, artist_id) REFERENCES Artists (service_id, artist_id) '
               f');'
               )
        return sql        
    
    def create_likes_table(self):
        sql = (f'CREATE TABLE IF NOT EXISTS Likes '
               f'(user_id integer, service_id integer, album_id varchar, '
               f'like_date timestamp, rating integer, ranking integer, '
               f'PRIMARY KEY (user_id, service_id, album_id), '
               f'FOREIGN KEY (user_id) REFERENCES Users (user_id), '
               f'FOREIGN KEY (service_id, album_id) REFERENCES Albums (service_id, album_id)'
               f');'
               )
        return sql     
    
    def create_user_albums_view(self):
        sql = (f'CREATE OR REPLACE VIEW UserAlbums AS '
               f'SELECT Artists.artist_name, Albums.album_name, Albums.release_date, Albums.categorization, '
               f'Likes.like_date, Likes.user_id '
               f'FROM Likes JOIN Albums '
               f'ON Likes.service_id = Albums.service_id AND Likes.album_id = Albums.album_id '
               f'JOIN Artists '
               f'ON Albums.service_id = Artists.service_id AND Albums.artist_id = Artists.artist_id'
               f';'
               )
        return sql
        
    def update_service_table(self, df, table_name, columns, pk_columns, service_id):
        values = ', '.join('(' + f'{self.dbify(service_id)}, ' + ', '.join(self.dbify(v) for v in r) + ')' for r in df[columns].values)
        sql = (f'INSERT INTO {table_name} (service_id, {", ".join(columns)}) '
               f'VALUES {values} '
               f'ON CONFLICT (service_id, {", ".join(pk_columns)}) '
               f'DO NOTHING '
               f';'
               )

        self.execute(sql)
        
    def update_artists(self, artists_df, service_id):
        columns = ['artist_id', 'artist_name']
        self.update_service_table(artists_df, 'Artists', columns, ['artist_id'], service_id)
        
    def update_albums(self, albums_df, service_id):
        columns = ['album_id', 'artist_id', 'album_name', 'album_type', 
                   'categorization', 'release_date', 'image_src', #'genres',
                   'track_list', 'skip_list'] #, 'replacement']
        self.update_service_table(albums_df, 'Albums', columns, ['album_id'], service_id)
        
    def update_likes(self, likes_df, service_id, user_id):
        columns = ['user_id', 'album_id', 'like_date'] #, 'rating', 'ranking']
        likes_df['user_id'] = user_id
        self.update_service_table(likes_df, 'Likes', columns, ['user_id', 'album_id'], service_id)
        
    def get_service_id(self, service_name):
        sql = f'SELECT service_id FROM Services WHERE service_name = {self.dbify(service_name)};'
        service_id = self.read_sql(sql).squeeze()
        return service_id
        
    def update_user(self, user):
        suids = {'Spotify': user.spotify_user_id,
                 'SoundCloud': user.soundcloud_username}
        service_user_ids = {self.get_service_id(s): suids[s] for s in suids}
        values = ', '.join(self.dbify(v) for v in [user.user_id, user.first_name, user.last_name,
                                                   service_user_ids, user.image_src])
        sql = (f'INSERT INTO Users (user_id, first_name, last_name, service_user_ids, image_src) '
               f'VALUES {values};'
               )
        self.execute(sql)
    
    def get_user_albums(self, user):
        sql = f'SELECT * FROM UserAlbums WHERE user_id = {self.dbify(user.user_id)}'
        albums_df = self.read_sql(sql)
        return albums_df