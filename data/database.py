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
        
        if sql:
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


    ''' table setup '''
    def create_tables(self):
        for sql in SQLer.create_tables():
            self.execute(sql, commit=False)
        self.execute()
            
    def drop_tables(self):
        for sql in SQLer.drop_tables():
            self.execute(sql, commit=False)
        self.execute()


    ''' view setup '''                    
    def create_views(self):
        for sql in SQLer.create_views() + SQLer.summarize_views():
            self.execute(sql, commit=False)
        self.execute()
        
    def drop_views(self):
        for sql in SQLer.drop_views():
            self.execute(sql, commit=False)
        self.execute()

        
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
        self.update_service_table(ownerships_df, 'ownerships', columns, ['user_id', 'album_id'], source_id=source_id, drop=True)
        
    def update_tracks(self, tracks_df, service_id):
        columns = ['track_id', 'track_name', 'track_duration', 'artist_ids', 'isrc', 'explicit']
        self.update_service_table(tracks_df, 'tracks', columns, ['track_id'], service_id=service_id)

    def update_soundtracks(self, tracks_df, service_id):
        columns = ['track_id', 'instrumentalness']
        self.update_service_table(tracks_df, 'tracks', columns, ['track_id'], service_id=service_id, update_only=True)

    def update_recordings(self, tracks_df):
        columns = ['isrc', 'iswc']
        self.update_service_table(tracks_df, 'recordings', columns, ['isrc'])

    def update_works(self, tracks_df):
        columns = ['iswc', 'release_year']
        self.update_service_table(tracks_df, 'works', columns, ['iswc'])
        
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
    # # def get_summary_of_ranks(self, user_id):    
    # #     # get a summary of what the user has
    # #     sql = (f"SELECT category, COUNT(CASE WHEN ranking IS NOT NULL THEN 1 END) AS ranked, "
    # #            f"COUNT(CASE WHEN ranking IS NULL THEN 1 END) AS unranked "
    # #            f"FROM albums_to_rank WHERE user_id = {user_id} AND category NOT IN ('single', 'ep', 'playlist') "
    # #            f"GROUP BY category "
    # #            f";")
    # #     summary_df = self.read_sql(sql)
    # #     return summary_df
      
    # # def get_albums_to_compare(self, user_id):
    # #     # for each category that has no ranked albums, select one and rank it as 1
    # #     summary_df = self.get_summary_of_ranks(user_id)

    # #     # might need to check that user has any albums?
    # #     unranked_categories = summary_df.query('ranked == 0')['category']
    # #     for category in unranked_categories:
    # #         sql = (f"SELECT source_id, album_id, album_name, artist_name FROM albums_to_rank "
    # #                f"WHERE category = '{category}' AND user_id = {user_id} "
    # #                f"ORDER BY RANDOM() LIMIT 1 "
    # #                f";"
    # #                )
    # #         albums_df = self.read_sql(sql)
    # #         source_id, album_id, album_name, artist_name = albums_df.iloc[0][['source_id', 'album_id', 'album_name', 'artist_name']]
    # #         sql = (f"UPDATE ownerships SET ranking = 1 WHERE user_id = '{user_id}' "
    # #                f"AND source_id = '{source_id}' AND album_id = '{album_id}' "
    # #                f";"
    # #                )
    # #         self.execute(sql)
            
    # #         # adjust the summary table
    # #         summary_df.loc[summary_df['category'] == category, ['ranked', 'unranked']] += 1, -1
        
    # #     # pick an album that is already ranked and has other albums in that category
    # #     categories_df = summary_df.query('unranked > 0')
    # #     if not categories_df.empty:
    # #         # select a category based on the distribution of all albums
    # #         category = categories_df.sample(weights=categories_df[['ranked', 'unranked']].sum(1))['category'].iloc[0]
    # #         sql = (f"WITH ranked_albums AS "
    # #                f"(SELECT source_id, album_id, album_name, artist_name, ranking, rating, category FROM albums_to_rank "
    # #                f"WHERE user_id = {user_id} AND category = '{category}'), "
                   
    # #                f"first_pick AS "
    # #                f"(SELECT source_id, album_id, album_name, artist_name, ranking, rating, category FROM ranked_albums "
    # #                f"WHERE ranking IS NOT NULL ORDER BY RANDOM() LIMIT 1), "

    # #                f"second_pick AS "
    # #                f"(SELECT source_id, album_id, album_name, artist_name, ranking, rating, category FROM ranked_albums "
    # #                f"WHERE (source_id, album_id) != (SELECT source_id, album_id FROM first_pick) "
    # #                f"ORDER BY RANDOM() LIMIT 1) " #AND category = (SELECT category FROM first_pick)
                   
    # #                f"SELECT * FROM first_pick UNION SELECT * FROM second_pick " 
    # #                f";"
    # #                )
    # #         albums_df = self.read_sql(sql)
            
    # #     else:
    # #         albums_df = DataFrame()
        
    # #     return albums_df, summary_df
    
    # # def update_album_rank(self, user_id, source_id, album_id, category, ranking):
    # #     # move other albums first
    # #     sql = (f"UPDATE ownerships SET ranking = ownerships.ranking + 1 "
    # #            f"FROM albums_to_rank "
    # #            f"WHERE ownerships.user_id = albums_to_rank.user_id "
    # #            f"AND ownerships.source_id = albums_to_rank.source_id "
    # #            f"AND ownerships.album_id = albums_to_rank.album_id "
    # #            f"AND albums_to_rank.user_id = {user_id} "
    # #            f"AND albums_to_rank.ranking >= {ranking} "
    # #            f"AND albums_to_rank.category = '{category}' "
    # #            f";"
    # #            )
    # #     self.execute(sql, commit=False)
        
    # #     # insert ranking of album
    # #     sql = (f"UPDATE ownerships SET ranking = {ranking} "
    # #            f"WHERE user_id = {user_id} AND source_id = '{source_id}' AND album_id = '{album_id}' "
    # #            f";"
    # #            )
    # #     self.execute(sql)
        
    def update_album_rating(self, user_id, source_id, album_id, rating):
        # need to ensure that rankings are adjusted -- can't have a B+ album above an A-
        sql = (f"UPDATE ownerships SET rating = {rating} WHERE user_id = {self.dbify(user_id)} "
               f"AND source_id = {self.dbify(source_id)} AND album_id = {self.dbify(album_id)} "
               f";"
               )
        self.execute(sql)

    def get_album_comparisons(self, user_id):
        sql = (f"WITH release_categories AS "
               f"(SELECT user_id, source_id, album_id, category "
               f"FROM ownerships JOIN album_categories USING(source_id, album_id) "
               f"WHERE user_id = {user_id} AND category NOT IN ('single', 'ep', 'playlist')), "
               
               f"availabe_categories AS "
               f"(SELECT user_id, category, COUNT(*) AS num_albums FROM release_categories "
               f"GROUP BY category, user_id), "
               
               f"owned_categories AS "
               f"(SELECT category FROM release_categories WHERE category IN "
               f"(SELECT category FROM availabe_categories WHERE num_albums > 1)), "
               
               f"first_pick AS "
               f"(SELECT source_id, album_id, category FROM release_categories "
               f"WHERE category IN (SELECT category FROM owned_categories) "
               f"ORDER BY RANDOM() LIMIT 1), "

               f"second_pick AS "
               f"(SELECT source_id, album_id, category FROM release_categories "
               f"WHERE category = (SELECT category FROM first_pick) "
               f"AND (source_id, album_id) != (SELECT source_id, album_id FROM first_pick) "
               f"ORDER BY RANDOM() LIMIT 1), "
               
               f"picks AS (SELECT * FROM first_pick UNION SELECT * FROM second_pick) "

               f"SELECT user_id, source_id, album_id, category, album_name, artist_names, ranking "
               f"FROM picks JOIN albums USING(source_id, album_id) "
               f"JOIN ownerships USING(source_id, album_id) JOIN sources USING(source_id) "
               f"JOIN album_artists USING(source_id, album_id) "
               f"LEFT JOIN release_battles USING(user_id, source_id, album_id) "
               f";"
               )
        albums_df = self.read_sql(sql)
        return albums_df
        
    def update_album_comparisons(self, user_id, source_id_1, album_id_1, source_id_2, album_id_2, winner):
        albums = [(source_id_1, album_id_1), (source_id_2, album_id_2)]
        source_id_w, album_id_w = albums[winner - 1]
        source_id_l, album_id_l = albums[2 - winner]
        # update winner
        sql = (f"UPDATE ownerships SET wins = COALESCE(wins, '[]'::jsonb) || {self.dbify([[source_id_l, album_id_l]])} "
               f"WHERE user_id = {user_id} AND source_id = {source_id_w} AND album_id = '{album_id_w}' "
               f";")
        self.execute(sql)

        # update loser
        sql = (f"UPDATE ownerships SET losses = COALESCE(losses, '[]'::jsonb) || {self.dbify([[source_id_w, album_id_w]])} "
               f"WHERE user_id = {user_id} AND source_id = {source_id_l} AND album_id = '{album_id_l}' "
               f";")
        self.execute(sql)

    def get_album_to_rate(self, user_id, unrated=False):
        wheres = ' AND rating IS NULL ' if unrated else ''
        sql = (f"SELECT source_id, album_id, artist_names, album_name, rating FROM ownerships "
               f"JOIN albums USING(source_id, album_id) JOIN sources USING(source_id) "
               f"JOIN album_artists USING(source_id, album_id) "
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
               f"FROM ownerships JOIN release_battles USING(user_id, source_id, album_id) "
               f"JOIN album_categories USING(source_id, album_id) "
               f"JOIN albums USING(source_id, album_id) JOIN sources USING(source_id) "
               f"JOIN album_artists USING(source_id, album_id) "
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
    
    def get_various_artist_id(self, service_id):
        sql = f'SELECT various_artist_id FROM services WHERE service_id = {self.dbify(service_id)};'
        various_artist_id = self.read_sql(sql).squeeze() ## squeeze may not be right here
        return various_artist_id

    def get_table_to_update(self, table_name, service_id=None, source_id=None, limit=1000):# limit=None):
        limits = f'LIMIT {limit}' if limit else ''
        wheres = f'WHERE service_id = {service_id}' if service_id else f'WHERE source_id = {source_id}' if source_id else ''
        sql = (f'SELECT * FROM update_{table_name} {wheres} {limits};')
        updates_df = self.read_sql(sql)
        return updates_df
    
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