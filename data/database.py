''' Database functionality '''

from datetime import datetime, date as dtdate
import json
import time
from collections import Counter

from pandas import read_sql, isna
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
        self.mark_time()
        username = NEON_USERNAME
        password = get_secret('NEON_PASSWORD')
        host = NEON_HOST
        db_name = NEON_DB_NAME
        url = f'postgresql://{username}:{password}.{host}/{db_name}?sslmode=require'
        
        print('connecting to database')
        engine = create_engine(url, pool_pre_ping=True,
                               connect_args={'keepalives': 1,
                                             'keepalives_idle': 30,
                                             'keepalives_interval': 10,
                                             'keepalives_count': 5})
        self.connection = engine.connect()
        print('connected')

    def disconnect(self):
        if self.connection:
            self.connection = None
            #self.connection.close()

    def reconnect(self):
        if time.time() - self.stopwatch >= self.reset_time:
            self.disconnect()
            self.connect()
        self.mark_time()

    def mark_time(self):
        self.stopwatch = time.time()

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
            
        elif isinstance(value, (datetime, dtdate)):
            value = "'" + value.strftime('%Y-%m-%d') + "'::date"
            
        elif isinstance(value, (list, dict)):
            value = "'" + json.dumps(self.jsonify(value)).replace("'", "''") + "'::jsonb"
            
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
        print('\tcreating tables')
        self.execute(SQLer.create_tables())
            
    def drop_tables(self):
        print('\tdropping tables [WARNING]')
        self.execute(SQLer.drop_tables())

    def create_views(self):
        print('\tcreating views')
        self.execute(SQLer.create_views() + SQLer.summarize_views())
        
    def materialize_views(self):
        print('\tmaterializing views')
        self.execute(SQLer.materialize_views())
    
    def refresh_views(self):
        print('\trefreshing views')
        self.execute(SQLer.refresh_views())
        
    def drop_views(self):
        print('\tdropping views [WARNING]')
        self.execute(SQLer.drop_views())

        
    ''' ensure the right configuration is in place '''
    def create_keywords(self, keywords_df):
        pass

    def add_service(self, service_name, sources=[], various_artists_id=None):
        sql = (f"WITH new_service_id AS "
               f"(INSERT INTO services (service_name, various_artist_id) "
               f"VALUES ({service_name}, {various_artists_id}) RETURNING service_id) "
               f"INSERT INTO sources (service_id, source_name) "
               f"VALUES (new_service_id, {self.dbify(sources)}) "
               f";"
               )
        self.execute(sql)
        
    def add_user(self, first_name, last_name, image_src=None):
        sql = (f"INSERT INTO users (first_name, last_name, image_src) "
               f"VALUES ({first_name}, {last_name}, {image_src}) "
               f"RETURNING user_id;"
               )
        user_id = self.execute(sql)
        return user_id
        
    def update_user(self, user_id, first_name=None, last_name=None, image_src=None, service_user_ids=None):
        attributes = ['first_name', 'last_name', 'image_src']
        attribute_cols = [first_name, last_name, image_src]
        set_attributes = [f'{a} = {self.dbify(c)}' for a, c in zip(attributes, attribute_cols) if c is not None]
        set_service = f'service_user_ids = service_user_ids || {self.dbify(service_user_ids)}' if service_user_ids else ''
        sets = ', '.join(set_attributes + set_service)
        sql = f'"UPDATE users SET {sets} WHERE user_id = {user_id};'
        self.execute(sql)

    ''' update data from pull '''
    def update_service_table(self, df, table_name, columns, pk_columns, service_id=None, source_id=None, update_only=False, drop=None,
                             least_dates=[], greatest_dates=[]):
        # filter out columns that aren't in the df
        columns = [c for c in columns if c in df.columns]
        s_id = ['service_id'] if service_id else ['source_id'] if source_id else []
        s_id_v = f'{self.dbify(service_id)}, ' if service_id else f'{self.dbify(source_id)}, ' if source_id else ''
        values = ', '.join('(' + s_id_v + ', '.join(self.dbify(v) for v in r) + ')' for r in df[columns].values)
        updatable = Counter(columns) != Counter(pk_columns)
            
        if drop or update_only:
            wheres = ' AND '.join(f'updt.{c} = {table_name}.{c}' for c in s_id + pk_columns)

        if drop or not update_only:
            insert_columns = ', '.join(s_id + columns)

        if not update_only:
            # upserting
            conflict_columns = ', '.join(s_id + pk_columns)
            excludes = ', '.join(f'{c} = EXCLUDED.{c}' for c in columns if c not in pk_columns)
            do_update = f'UPDATE SET {excludes}' if updatable else 'NOTHING'
            sql = (f'INSERT INTO {table_name} ({insert_columns}) VALUES {values} '
                   f'ON CONFLICT ({conflict_columns}) DO {do_update} '
                   )
        else:
            # updating
            if updatable:
                sets = ', '.join(self.set_update_column(c, 'updt', '>' if c in greatest_dates else '<' if c in least_dates else '=') for c in columns if c not in s_id + pk_columns)
                ases = ', '.join(s_id + columns)
                sql = (f'UPDATE {table_name} SET {sets} FROM (VALUES {values}) '
                       f'AS updt({ases}) WHERE {wheres} '
                       )
            else:
                sql = None
                
        if sql:
            if drop:
                drops = ', '.join(drop)
                sql = (f"WITH upsert AS ({sql} RETURNING {conflict_columns}) "
                       f"DELETE FROM {table_name} WHERE ({drops}) IN (SELECT DISTINCT {drops} FROM upsert) "
                       f"AND ({conflict_columns}) NOT IN (SELECT {conflict_columns} FROM upsert) "
                       )
            sql += ';'
            
        self.execute(sql)
        
    def set_update_column(self, column, alias, comparison='='):
        match comparison:
            case '=':
                update = f'{alias}.{column}'
            case '>':
                update = f'GREATEST({column}, {alias.column})'
            case '<':
                update = f'LEAST({column}, {alias.column})'
        update_column = f'{column} = {update}'
        return update_column

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
                   'track_uris', 'upc']
        self.update_service_table(albums_df, 'albums', columns, ['album_uri'], source_id=source_id)
        
    def update_ownerships(self, ownerships_df, source_id, user_id):
        columns = ['user_id', 'album_uri', 'like_date']
        ownerships_df['user_id'] = user_id
        drop = ['user_id', 'source_id']
        self.update_service_table(ownerships_df, 'ownerships', columns, ['user_id', 'album_uri'], source_id=source_id,
                                  drop=drop)
        
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

    def update_upcs(self, albums_df):
        columns = ['source_id', 'album_uri', 'upc']
        self.update_service_table(albums_df, 'albums', columns, ['source_id', 'album_uri'])
        
    def update_barcodes(self, albums_df):
        columns = ['upc', 'release_type']
        self.update_service_table(albums_df, 'barcodes', columns, ['upc'])

    def update_billboard(self, peaks_df, start_date, end_date):
        columns = ['credit_names', 'album_title', 'peak_position']
        self.update_service_table(peaks_df, 'billboard', columns, ['credit_names', 'album_title'])
        self.update_data_updates('billboard', start_date, end_date)

    def update_critics(self, lists_df):
        columns = ['critic_name', 'list_year', 'list_position', 'album_name', 'artist_names']
        self.update_service_table(lists_df, 'critics', columns, ['critic_name', 'list_year', 'list_position'])
        
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

    def get_album_comparisons(self, user_id, excluded_categories=['single', 'playlist'], allow_repeats=False):
        if excluded_categories:
            excludes = 'AND category NOT IN (' + ', '.join(f"'{c}'" for c in excluded_categories) + ') '
        else:
            excludes = ''
            
        if allow_repeats:
            owned_with = ('owned_categories AS '
                          '(SELECT category FROM release_categories WHERE category IN '
                          '(SELECT category FROM availabe_categories WHERE num_albums > 1)), '
                          )
            first_pick_where = 'WHERE category IN (SELECT category FROM owned_categories) '
            second_pick_where = ''

        else:
            owned_with = ''
            first_pick_where = 'JOIN availabe_categories USING (user_id, category) WHERE num_played < num_albums '            
            second_pick_where = 'AND NOT (SELECT played FROM first_pick) @> jsonb_build_array(source_id, album_uri) '

        sql = (f"WITH release_categories AS "
               f"(SELECT user_id, source_id, album_uri, category, "
               f"COALESCE(wins || losses, jsonb_build_array(NULL)) AS played, "
               f"COALESCE(jsonb_array_length(wins || losses), 0) AS num_played "
               f"FROM ownerships JOIN album_categories USING (source_id, album_uri) "
               f"WHERE user_id = {user_id} {excludes}), "
               
               f"availabe_categories AS "
               f"(SELECT user_id, category, COUNT(*) AS num_albums FROM release_categories "
               f"GROUP BY category, user_id), "
               
               f"{owned_with} "
              
               f"first_pick AS "
               f"(SELECT source_id, album_uri, category, played FROM release_categories "
               f"{first_pick_where} "
               f"ORDER BY RANDOM() LIMIT 1), "

               f"second_pick AS "
               f"(SELECT source_id, album_uri, category FROM release_categories "
               f"WHERE category = (SELECT category FROM first_pick) "
               f"AND (source_id, album_uri) != (SELECT source_id, album_uri FROM first_pick) "
               f"{second_pick_where} "
               f"ORDER BY RANDOM() LIMIT 1), "
               
               f"picks AS (SELECT source_id, album_uri, category FROM first_pick "
               f"UNION SELECT source_id, album_uri, category FROM second_pick)  "

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
        
        # update winner and loser
        where_w = f"{user_id}, {source_id_w}, '{album_uri_w}'"
        winner = self.dbify([source_id_w, album_uri_w])
        where_l = f"{user_id}, {source_id_l}, '{album_uri_l}'"
        loser = self.dbify([source_id_l, album_uri_l])
        sqls = [(f"WITH win_list_w AS "
                 f"(SELECT jsonb_array_elements(wins) AS win_value FROM ownerships "
                 f"WHERE (user_id, source_id, album_uri) = ({where_w})), "
                      
                 f"loss_list_w AS "
                 f"(SELECT jsonb_array_elements(losses) AS loss_value FROM ownerships "
                 f"WHERE (user_id, source_id, album_uri) = ({where_w})) "
                      
                 f"UPDATE ownerships SET "
                 f"wins = jsonb_build_array({loser}) || "
                 f"COALESCE((SELECT jsonb_agg(win_value) FROM win_list_w WHERE win_value <> {loser}), '[]'::jsonb), "
                 f"losses = (SELECT jsonb_agg(loss_value) FROM loss_list_w WHERE loss_value <> {loser}) "
                 f"WHERE (user_id, source_id, album_uri) = ({where_w}) "
                 f";"
                 ),
                (f"WITH win_list_l AS "
                 f"(SELECT jsonb_array_elements(wins) AS win_value FROM ownerships "
                 f"WHERE (user_id, source_id, album_uri) = ({where_l})), "
                     
                 f"loss_list_l AS "
                 f"(SELECT jsonb_array_elements(losses) AS loss_value FROM ownerships "
                 f"WHERE (user_id, source_id, album_uri) = ({where_l})) "
                      
                 f"UPDATE ownerships SET "
                 f"wins = (SELECT jsonb_agg(win_value) FROM win_list_l WHERE win_value <> {winner}), "
                 f"losses = jsonb_build_array({winner}) || "
                 f"COALESCE((SELECT jsonb_agg(loss_value) FROM loss_list_l WHERE loss_value <> {winner}), '[]'::jsonb) "
                 f"WHERE (user_id, source_id, album_uri) = ({where_l}) "
                 f";"
                 ),
                ]
        self.execute(sqls)

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
        tracks_df = self.get_table_to_update('compilations')
        return tracks_df
    
    def get_upcs_to_update(self):
        albums_df = self.get_table_to_update('upcs')
        return albums_df
        
    def get_barcodes_to_update(self):
        tracks_df = self.get_table_to_update('barcodes')
        return tracks_df
    
    def get_billboard_to_update(self):
        start_date, end_date = self.get_data_updates('billboard')
        return start_date, end_date
    
    def get_critics_to_update(self):
        sql = (f"SELECT DISTINCT critic_name, list_year FROM critics;")
        excludes = self.read_sql(sql)
        return excludes

    def get_user_ids(self):
        sql = (f'SELECT user_id FROM users;')
        user_ids = self.read_sql(sql)['user_id'].to_list()
        return user_ids
    
    def get_user(self, user_id):
        sql = (f'SELECT * FROM users '
               f'WHERE user_id = {self.dbify(user_id)}'
               f';'
               )
        user_s = self.read_sql(sql).squeeze()
        return user_s
   

    ''' get data for push '''
    def get_user_albums(self, user_id, min_ranking=None, min_rating=None, categories=None,
                        min_duration=None, max_duration=None, explicit=None, release_year=None, release_decade=None,
                        min_chart_peak=None, min_stars=None):
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
        if min_chart_peak:
            wheres.append(f'peak_position <= {min_chart_peak}')
        if min_stars:
            wheres.append(f'stars >= {min_stars}')
            
        sql = f'SELECT * FROM user_albums WHERE user_id = {user_id}{" AND ".join(wheres)}'
        albums_df = self.read_sql(sql)
        return albums_df
    
    def get_random_album(self, user_id, **kwargs):
        albums_df = self.get_user_albums(user_id, **kwargs)
        album_s = albums_df.sample(1).squeeze()
        return album_s