''' Database functionality '''

from datetime import datetime
import json
import time

from psycopg2 import OperationalError
from pandas import DataFrame, Series, read_sql, isna
from numpy import integer, floating
from sqlalchemy import create_engine, text

from common.secret import get_secret
from common.structure import NEON_DB_NAME, NEON_USERNAME, NEON_HOST
from data.sqls import SQLer

class Neon:
    reset_time = 120
    good_release_types = ['studio', 'compilation', 'soundtrack', 'score']
    medium_release_types = ['ep', 'playlist']
    bad_release_types = ['single']

    def __init__(self):
        self.connection = None
        self.stopwatch = 0
    
    def connect(self):
        self.stopwatch = time.time()
        username = NEON_USERNAME
        password = get_secret('NEON_PASSWORD')
        host = NEON_HOST
        db_name = NEON_DB_NAME
        url = f'postgresql://{username}:{password}.{host}/{db_name}?sslmode=require'
        engine = create_engine(url, pool_pre_ping=True,
                               connect_args={'keepalives': 1,
                                             'keepalives_idle': 30,
                                             'keepalives_interval': 10,
                                             'keepalives_count': 5})
        self.connection = engine.connect()
        
    def disconnect(self):
        if self.connection:
            self.connection = None
            #self.connection.close()

    def reconnect(self):
        if time.time() - self.stopwatch >= self.reset_time:
            self.disconnect()
            self.connect()

    ''' basic DB functions '''    
    def execute(self, sql=None, commit=True):
        self.reconnect()

        if isinstance(sql, list):
            for s in sql:
                self.execute(s, commit=False)
        elif isinstance(sql, str):
            self.connection.execute(text(sql))

        if commit:
            self.connection.commit()
            
    def read_sql(self, sql):
        self.reconnect()
        return read_sql(sql, self.connection)
 

    ''' string functions '''
    def dbify(self, value):
        if isinstance(value, str):
            value = "'" + value.replace("'", "''") + "'"
            
        elif isinstance(value, datetime):
            value = "'" + value.strftime('%Y-%m-%d') + "'::date"
            
        elif isinstance(value, (list, dict)):
            value = "'" + json.dumps(self.jsonify(value)) + "'::jsonb"
            
        elif (value is None) or isna(value):
            value = 'NULL'

        return str(value)

    def jsonify(self, value):
        if isinstance(value, integer):
            value = int(value)
        elif isinstance(value, floating):
            value = float(value)
        elif isinstance(value, list):
            value = [self.jsonify(v) for v in value]
        elif isinstance(value, dict):    
            value = {self.jsonify(v): self.jsonify(value[v]) for v in value}
        return value
        
    
    def regify(self, values):
        if isinstance(values, (int, str, float)):
            values = [str(values)]
            
        return '%(' + '|'.join(self.dbify(v.lower()) for v in values) + ')%'


    ''' table and view setup '''
    def create_tables(self):
        self.execute(SQLer.create_tables())
            
    def drop_tables(self):
        self.execute(SQLer.drop_tables())

    def create_views(self):
        self.execute(SQLer.create_views() + SQLer.summarize_views())
        
    def materialize_views(self):
        self.execute(SQLer.materialize_views())
    
    def refresh_views(self):
        self.execute(SQLer.refresh_views())
        
    def drop_views(self):
        self.execute(SQLer.drop_views())

        
    ''' update data from pull '''
    def update_service_table(self, df, table_name, columns, pk_columns, service_id=None, source_id=None, update_only=False, drop=False):
        s_id = ['service_id'] if service_id else ['source_id'] if source_id else []
        s_id_v = f'{self.dbify(service_id)}, ' if service_id else f'{self.dbify(source_id)}, ' if source_id else ''
        values = ', '.join('(' + s_id_v + ', '.join(self.dbify(v) for v in r) + ')' for r in df[columns].values)
            
        if drop or update_only:
            wheres = ' AND '.join(f'updt.{c} = {table_name}.{c}' for c in s_id + pk_columns)

        if drop or not update_only:
            insert_columns = ', '.join(s_id + columns)

        if not update_only:
            # upserting
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
            sql = (f'UPDATE {table_name} SET {sets} FROM (VALUES {values}) '
                   f'AS updt({ases}) WHERE {wheres} '
                   f';'
                   )
            
        self.execute(sql)

        if drop:
            pass
            # # # remove values that are no longer applicable
            # # sql = (f'DELETE FROM {table_name} WHERE NOT EXISTS (SELECT FROM (VALUES {values}) AS updt({insert_columns}) '
            # #        f'WHERE {wheres})'
            # #        f';'
            # #        )
            # # self.execute(sql)

    def update_data_updates(self, table_name, start_date=None, end_date=None):
        sql = (f"INSERT INTO _data_updates (table_name, start_date, end_date) "
               f"VALUES ('{table_name}', {self.dbify(start_date)}, {self.dbify(end_date)}) "
               f"ON CONFLICT (table_name) DO UPDATE SET "
               f"start_date = LEAST(_data_updates.start_date, EXCLUDED.start_date), "
               f"end_date = GREATEST(_data_updates.end_date, EXCLUDED.end_date) "
               f"WHERE _data_updates.table_name = '{table_name}' "
               f";"
               )
        self.execute(sql)
            
    def update_artists(self, artists_df, service_id):
        columns = ['artist_uri', 'artist_name']
        self.update_service_table(artists_df, 'artists', columns, ['artist_uri'], service_id=service_id)
        
    def update_albums(self, albums_df, source_id):
        columns = ['album_uri', 'artist_uris', 'album_name', 'album_type', 
                   'release_date', 'image_src', 'album_duration',
                   'track_uris', 'replacement', 'upc']
        self.update_service_table(albums_df, 'albums', columns, ['album_uri'], source_id=source_id)
        
    def update_ownerships(self, ownerships_df, source_id, user_id):
        columns = ['user_id', 'album_uri', 'like_date']
        ownerships_df['user_id'] = user_id
        self.update_service_table(ownerships_df, 'ownerships', columns, ['user_id', 'album_uri'], source_id=source_id, drop=True)
        
    def update_tracks(self, tracks_df, service_id):
        columns = ['track_uri', 'track_name', 'track_duration', 'artist_uris', 'isrc', 'explicit']
        self.update_service_table(tracks_df, 'tracks', columns, ['track_uri'], service_id=service_id)

    def update_soundtracks(self, tracks_df, service_id):
        columns = ['track_uri', 'instrumentalness']
        self.update_service_table(tracks_df, 'tracks', columns, ['track_uri'], service_id=service_id, update_only=True)

    def update_recordings(self, tracks_df):
        columns = ['isrc', 'iswc']
        self.update_service_table(tracks_df, 'recordings', columns, ['isrc'])

    def update_works(self, tracks_df):
        columns = ['iswc', 'release_year']
        self.update_service_table(tracks_df, 'works', columns, ['iswc'])
        
    def update_barcodes(self, albums_df):
        columns = ['upc', 'release_type']
        self.update_service_table(albums_df, 'barcodes', columns, ['upc'])

    def update_billboard(self, peaks_df, start_date, end_date):
        columns = ['credit_names', 'album_name', 'peak_position']
        self.update_service_table(peaks_df, 'billboard', columns, ['credit_names', 'album_name'])
        self.update_data_updates('billboard', start_date, end_date)

    def update_user(self, user):
        columns = ['user_id', 'first_name', 'last_name', 'service_user_ids', 'image_src']
        self.update_service_table(user, 'users', columns, ['user_id'])
        
    # # def update_series(self, user_id, series_name, artist_uri=None, artist_name=None, album_name_pattern=None, album_not_pattern=None):
    # #     artist_yes = f'artist_uris ? {self.dbify(artist_uri)}' if artist_uri else None
    # #     artist_maybe = f'lower(artist_names) SIMILAR TO {self.regify(album_name_pattern)}' if artist_name else None
    # #     album_yes = f'lower(album_names) SIMILAR TO {self.regify(album_name_pattern)}' if album_name_pattern else None
    # #     album_no = f'lower(album_name) NOT SIMILAR TO {self.regify(album_not_pattern)}' if album_name_pattern else None
        
    # #     sql = (f'WITH new_series AS (SELECT jsonb_agg(row_data) AS album_list '
    # #            f'FROM (SELECT jsonb_build_array(source_id, album_uri) AS row_data '
    # #            f'FROM user_albums WHERE {" AND ".join(c for c in [artist_yes, artist_maybe, album_yes, album_no] if c is not None)} '
    # #            f'ORDER BY release_date)) '

    # #            f'INSERT INTO series (user_id, series_name, album_list) '
    # #            f'VALUES ({user_id}, {self.dbify(series_name)}, (SELECT album_list FROM new_series)) '
    # #            f';'
    # #            )
    # #     self.execute(sql)


    ''' manual changes '''       
    def update_album_rating(self, user_id, source_id, album_uri, rating):
        # need to ensure that rankings are adjusted -- can't have a B+ album above an A-
        sql = (f"UPDATE ownerships SET rating = {rating} WHERE user_id = {self.dbify(user_id)} "
               f"AND source_id = {self.dbify(source_id)} AND album_uri = {self.dbify(album_uri)} "
               f";"
               )
        self.execute(sql)

    def get_album_comparisons(self, user_id):
        sql = (f"WITH release_categories AS "
               f"(SELECT user_id, source_id, album_uri, category "
               f"FROM ownerships JOIN album_categories USING (source_id, album_uri) "
               f"WHERE user_id = {user_id} AND category NOT IN ('single', 'ep', 'playlist')), "
               
               f"availabe_categories AS "
               f"(SELECT user_id, category, COUNT(*) AS num_albums FROM release_categories "
               f"GROUP BY category, user_id), "
               
               f"owned_categories AS "
               f"(SELECT category FROM release_categories WHERE category IN "
               f"(SELECT category FROM availabe_categories WHERE num_albums > 1)), "
               
               f"first_pick AS "
               f"(SELECT source_id, album_uri, category FROM release_categories "
               f"WHERE category IN (SELECT category FROM owned_categories) "
               f"ORDER BY RANDOM() LIMIT 1), "

               f"second_pick AS "
               f"(SELECT source_id, album_uri, category FROM release_categories "
               f"WHERE category = (SELECT category FROM first_pick) "
               f"AND (source_id, album_uri) != (SELECT source_id, album_uri FROM first_pick) "
               f"ORDER BY RANDOM() LIMIT 1), "
               
               f"picks AS (SELECT * FROM first_pick UNION SELECT * FROM second_pick) "

               f"SELECT user_id, source_id, album_uri, category, album_name, artist_names, ranking "
               f"FROM picks JOIN albums USING (source_id, album_uri) "
               f"JOIN ownerships USING (source_id, album_uri) JOIN sources USING (source_id) "
               f"JOIN album_artists USING (source_id, album_uri) "
               f"LEFT JOIN release_battles USING (user_id, source_id, album_uri) "
               f";"
               )
        albums_df = self.read_sql(sql)
        return albums_df
        
    def update_album_comparisons(self, user_id, source_id_1, album_uri_1, source_id_2, album_uri_2, winner):
        albums = [(source_id_1, album_uri_1), (source_id_2, album_uri_2)]
        source_id_w, album_uri_w = albums[winner - 1]
        source_id_l, album_uri_l = albums[2 - winner]
        # update winner
        sql = (f"UPDATE ownerships SET wins = COALESCE(wins, '[]'::jsonb) || {self.dbify([[source_id_l, album_uri_l]])} "
               f"WHERE user_id = {user_id} AND source_id = {source_id_w} AND album_uri = '{album_uri_w}' "
               f";")
        self.execute(sql)

        # update loser
        sql = (f"UPDATE ownerships SET losses = COALESCE(losses, '[]'::jsonb) || {self.dbify([[source_id_w, album_uri_w]])} "
               f"WHERE user_id = {user_id} AND source_id = {source_id_l} AND album_uri = '{album_uri_l}' "
               f";")
        self.execute(sql)

    def get_album_to_rate(self, user_id, unrated=False):
        wheres = ' AND rating IS NULL ' if unrated else ''
        sql = (f"SELECT source_id, album_uri, artist_names, album_name, rating FROM ownerships "
               f"JOIN albums USING (source_id, album_uri) JOIN sources USING (source_id) "
               f"JOIN album_artists USING (source_id, album_uri) "
               f"WHERE user_id = {user_id}{wheres} "
               f"ORDER BY RANDOM() LIMIT 1"
               f";"
               )
        album_s = self.read_sql(sql).squeeze()
        return album_s

    def get_album_summary(self, user_id, max_ranking=5):
        categories = ['studio', 'compilation', 'soundtrack', 'score']
        cases = ' '.join(f"WHEN '{category}' THEN {i}" for i, category in enumerate(categories)) + f' ELSE {len(categories)}'
        sql = (f"SELECT artist_names, album_name, category, ranking, rating "
               f"FROM ownerships JOIN release_battles USING (user_id, source_id, album_uri) "
               f"JOIN album_categories USING (source_id, album_uri) "
               f"JOIN albums USING (source_id, album_uri) JOIN sources USING (source_id) "
               f"JOIN album_artists USING (source_id, album_uri) "
               f"WHERE user_id = {user_id} AND release_battles.ranking <= {max_ranking} "
               f"ORDER BY CASE category {cases} END, ranking ASC, rating DESC "
               f";"
               )
        albums_df = self.read_sql(sql)
        return albums_df

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
    
    def get_various_artist_uri(self, service_id):
        sql = f'SELECT various_artist_uri FROM services WHERE service_id = {self.dbify(service_id)};'
        various_artist_uri = self.read_sql(sql).squeeze() ## squeeze may not be right here
        return various_artist_uri

    def get_table_to_update(self, table_name, service_id=None, source_id=None, limit=1000):# limit=None):
        limits = f'LIMIT {limit}' if limit else ''
        wheres = f'WHERE service_id = {service_id}' if service_id else f'WHERE source_id = {source_id}' if source_id else ''
        sql = (f'SELECT * FROM update_{table_name} {wheres} {limits};')
        updates_df = self.read_sql(sql)
        return updates_df
    
    def get_data_updates(self, table_name):
        sql = (f"SELECT start_date, end_date FROM _data_updates "
               f"WHERE table_name = '{table_name}' "
               f";"
               )
        data_updates_df = self.read_sql(sql)
        if len(data_updates_df):
            start_date, end_date = data_updates_df.iloc[0][['start_date', 'end_date']]
        else:
            start_date = end_date = None
        return start_date, end_date
    
    def get_tracks_to_update(self, service_id):
        tracks_df = self.get_table_to_update('tracks', service_id=service_id)
        return tracks_df

    def get_recordings_to_update(self):
        tracks_df = self.get_table_to_update('recordings')#, limit=50)
        return tracks_df

    def get_works_to_update(self):
        tracks_df = self.get_table_to_update('works')#, limit=50)
        return tracks_df

    def get_artists_to_update(self, service_id):
        artists_df = self.get_table_to_update('artists', service_id=service_id)
        return artists_df

    def get_soundtracks_to_update(self, service_id):
        tracks_df = self.get_table_to_update('soundtracks', service_id=service_id)
        return tracks_df

    def get_compilations_to_update(self):
        tracks_df = self.get_table_to_update('compilations', limit=300)
        return tracks_df

    def get_barcodes_to_update(self):
        tracks_df = self.get_table_to_update('barcodes', limit=300)
        return tracks_df
    
    def get_billboard_to_update(self):
        start_date, end_date = self.get_data_updates('billboard')
        return start_date, end_date

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
    def get_user_albums(self, user_id, min_ranking=None, min_rating=None, categories=None,
                        min_duration=None, max_duration=None, explicit=None, release_year=None, release_decade=None):
        wheres = []
        if min_ranking:
            wheres.append(f'ranking <= {min_ranking}')
        if min_rating:
            wheres.append(f'rating >= {min_rating}')
        if categories:
            cats = ', '.join(f"'{c}'" for c in categories)
            wheres.append(f'category IN ({cats})')
        if min_duration or max_duration:
            if min_duration and not max_duration:
                wheres.append(f'play_duration >= {min_duration}')
            elif max_duration and not min_duration:
                wheres.append(f'play_duration <= {max_duration}')
            else:
                wheres.append(f'play_duration BETWEEN {min_duration} AND {max_duration}')
        if release_year:
            wheres.append(f'EXTRACT(YEAR FROM release_date) = {release_year}')
        elif release_decade:
            wheres.append(f"COALESCE(release_decades, jsonb_build_array(FLOOR(EXTRACT(YEAR FROM release_date) / 10) * 10)) ? '{release_decade}'")
        if explicit is not None:
            wheres.append(f'COALESCE(explicit, FALSE) = {explicit}')
            
        sql = f'SELECT * FROM user_albums WHERE user_id = {user_id}{" AND ".join(wheres)}'
        albums_df = self.read_sql(sql)
        return albums_df
    
    def get_random_album(self, user_id, min_ranking=None, min_rating=None, categories=None,
                        min_duration=None, max_duration=None, explicit=None, release_year=None, release_decade=None):
        albums_df = self.get_user_albums(user_id, min_ranking, min_rating, categories,
                                         min_duration, max_duration, explicit, release_year, release_decade)
        album_s = albums_df.sample(1).squeeze()
        return album_s