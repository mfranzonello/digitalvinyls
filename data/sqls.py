''' Database tables and views '''

class SQLer:
    tables = [{'name': 'services',
               'columns': [['service_id', 'serial'],
                           ['service_name', 'varchar'],
                           ['various_artist_uri', 'varchar'],
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
                           ['artist_uri', 'varchar'],
                           ['artist_name', 'varchar'],
                           ],
               'pk': ['service_id', 'artist_uri'],
               'fks': [[['service_id'], 'services'],
                       ],
               },
              {'name': 'albums',
               'columns': [['source_id', 'integer'],
                           ['album_uri', 'varchar'],
                           ['artist_uris', 'jsonb'],
                           ['album_name', 'varchar'],
                           ['album_type', 'varchar'],
                           ['genres', 'jsonb'],
                           ['release_date', 'timestamp'],
                           ['image_src', 'varchar'],
                           ['track_uris', 'jsonb'],
                           ['album_duration', 'numeric'],
                           ['upc', 'varchar'],
                           ['release_id', 'serial']
                           ],         
               'pk': ['source_id', 'album_uri'],
               'fk': [[['source_id'], 'sources'],
                      ],
               },
              {'name': 'ownerships',
               'columns': [['user_id', 'integer'],
                           ['source_id', 'integer'],
                           ['album_uri', 'varchar'],
                           ['like_date', 'timestamp'],
                           ['rating', 'integer'],
                           ],
               'pk': ['user_id', 'source_id', 'album_uri'],
               'fks': [[['user_id'], 'users'],
                       [['source_id', 'album_uri'], 'albums'],
                       ],
               },
              {'name': 'tracks',
               'columns': [['service_id', 'integer'],
                           ['track_uri', 'varchar'],
                           ['track_name', 'varchar'],
                           ['track_duration', 'numeric'],
                           ['artist_uris', 'jsonb'],
                           ['isrc', 'varchar'],
                           ['explicit', 'boolean']
                           ],
               'pk': ['service_id', 'track_uri'],
               'fks': [[['service_id'], 'services'],
                       ],
               },
              {'name': 'series',
               'columns': [['user_id', 'integer'],
                           ['series_name', 'varchar'],
                           ['album_list', 'jsonb'], # --> [[source_id, [album_uris]], [...]]
                           ],
               'pk': ['user_id', 'series_name'],
               'fk': [[['user_id'], 'users']]
               },
              {'name': 'replacements',
               'columns': [['source_id', 'integer'],
                           ['album_uri', 'varchar'],
                           ['replace_source_id', 'integer'],
                           ['replace_album_uri', 'varchar'],
                           ],
               'pk': ['source_id', 'album_uri'],
               'fks': [[['source_id', 'album_uri'], 'albums'],
                       [{'replacement_source_id': 'source_id', 'replacement_album_uri': 'album_uri'}, 'albums'],
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
              {'name': 'recordings',
               'columns': [['isrc', 'varchar'],
                           ['iswc', 'varchar'],
                           ],
               'pk': ['isrc'],
               },
              {'name': 'works',
               'columns': [['iswc', 'varchar'],
                           ['release_year', 'date'],
                           ],
               'pk': ['iswc'],
               },
              {'name': 'billboard',
               'columns': [['album_title', 'varchar'],
                           ['credit_names', 'varchar'],
                           ['peak_position', 'integer'],
                           ],
               'pk': ['album_title', 'credit_names'],
               },
              {'name': 'critics',
               'columns': [['critic_name', 'varchar'],
                           ['list_year', 'integer'],
                           ##['list_type', 'varchar'], #all_time, annual
                           ['list_position', 'int'],
                           ['album_name', 'varchar'],
                           ['artist_names', 'jsonb'],
                           ],
               'pk': ['critic_name', 'list_year', 'list_position'],            
               },
              {'name': '_data_updates',
               'columns': [['table_name', 'varchar'],
                           ['start_date', 'date'],
                           ['end_date', 'date'],
                           ],
               },
              ]
  
    views = [{'name': 'compilations',
              'sql': (f"WITH full_tracks AS "
                      f"(SELECT service_id, track_uri, release_year FROM tracks "
                      f"JOIN recordings USING (isrc) JOIN works USING (iswc)), "
                      
                      f"albums_expanded AS "
                      f"(SELECT source_id, album_uri, release_date, arr.track_uri FROM albums, "
                      f"jsonb_array_elements_text(track_uris) as arr(track_uri)) "
                      
                      f"SELECT source_id, album_uri, "
                      f"jsonb_agg(DISTINCT COALESCE(release_year, EXTRACT(YEAR FROM release_date))) AS release_years, "
                      f"jsonb_agg(DISTINCT COALESCE(floor(release_year / 10) * 10, "
                      f"floor(EXTRACT(YEAR FROM release_date) / 10) * 10)) AS release_decades "
                      f"FROM albums_expanded "
                      f"JOIN sources USING (source_id) LEFT JOIN full_tracks USING (service_id, track_uri) "
                      f"GROUP BY source_id, album_uri "
                      ),
              },
             {'name': 'explicit_albums',
              'sql': (f"SELECT source_id, album_uri, BOOL_OR(explicit) AS explicit "
                      f"FROM albums JOIN sources USING (source_id) "
                      f"JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_uris ? tracks.track_uri "
                      f"GROUP BY source_id, album_uri HAVING BOOL_OR(explicit) = TRUE "
                      ),
              },
             {'name': 'album_categories',
              'sql': (f"WITH regex_soundtrack AS (SELECT '%(' || string_agg(phrase, '|') || ')%' "
                      f"AS soundtrack_words FROM keywords WHERE keyword = 'soundtrack'), "
               
                      f"min_score AS (SELECT phrase::numeric AS min_instrumentalness FROM keywords WHERE keyword = 'score_instrumental'), "

                      f"min_tracks AS (SELECT phrase::numeric AS min_ep_tracks FROM keywords WHERE keyword = 'min_ep_tracks'), "
               
                      f"min_span AS (SELECT phrase::numeric AS min_release_span FROM keywords WHERE keyword = 'min_release_span'), "
               
                      f"mbid_soundtracks AS (SELECT upc, release_type FROM barcodes), "

                      f"instrumentals AS (SELECT source_id, album_uri, AVG(instrumentalness) AS instrumentalness "
                      f"FROM albums JOIN sources USING (source_id) "
                      f"JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_uris ? tracks.track_uri "
                      f"GROUP BY source_id, album_uri), "
               
                      f"release_years AS (SELECT source_id, album_uri, max(release_year) - min(release_year) AS release_span FROM albums "
                      f"JOIN sources USING (source_id) JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_uris ? tracks.track_uri "
                      f"JOIN recordings USING (isrc) JOIN works USING (iswc) "
                      f"GROUP BY source_id, album_uri) "
               
                      f"SELECT source_id, album_uri, "
                      f"CASE WHEN album_type = 'single' AND jsonb_array_length(track_uris) >= min_ep_tracks THEN 'ep' "
                      f"WHEN album_type in ('album', 'compilation') THEN "
                      f"(CASE WHEN lower(album_name) SIMILAR TO soundtrack_words OR release_type = 'soundtrack' THEN "
                      f"(CASE WHEN instrumentalness >= min_instrumentalness THEN 'score' ELSE 'soundtrack' END) "
                      f"WHEN release_type = 'compilation' AND release_span >= min_release_span THEN 'compilation' "
                      f"WHEN album_type = 'album' THEN 'studio' ELSE album_type END) ELSE album_type END AS category "
                      f"FROM albums LEFT JOIN instrumentals USING (source_id, album_uri) "
                      f"LEFT JOIN release_years USING (source_id, album_uri) LEFT JOIN mbid_soundtracks USING (upc), "
                      f"regex_soundtrack, min_tracks, min_span, min_score "
                      ),
              },
             {'name': 'auto_skips',
              'sql': (f"WITH regex_good_repeat AS (SELECT '%(' || string_agg(phrase, '|') || ')%' "
                      f"AS good_repeat FROM keywords WHERE keyword = 'good_repeat'), "
                      f"regex_bad_repeat AS (SELECT '%(' || string_agg(phrase, '|') || ')%' "
                      f"AS bad_repeat FROM keywords WHERE keyword = 'bad_repeat'), "
                      f"regex_good_short AS (SELECT '%(' || string_agg(phrase, '|') || ')%' "
                      f"AS good_short FROM keywords WHERE keyword = 'good_short'), "
                      f"min_track_time AS (SELECT phrase::numeric AS min_seconds FROM keywords WHERE keyword = 'min_track_time'), "
                      f"album_tracks AS (SELECT source_id, album_uri, elem.track_uri, elem.track_num "
                      f"FROM albums, jsonb_array_elements_text(track_uris) WITH ORDINALITY AS elem(track_uri, track_num)), "
               
                      f"album_track_uris AS (SELECT source_id, album_uri, track_uri, track_num, track_name FROM album_tracks "
                      f"JOIN sources USING (source_id) JOIN tracks USING (service_id, track_uri)) "
               
                      f"SELECT at1.source_id, at1.album_uri, at1.track_uri "
                      f"FROM album_track_uris AS at1 JOIN album_track_uris AS at2 ON "
                      f"at1.source_id = at2.source_id AND at1.album_uri = at2.album_uri AND "
                      f"at1.track_name LIKE at2.track_name || '%' AND at1.track_num > at2.track_num "
                      f"JOIN albums ON at1.source_id = albums.source_id AND at1.album_uri = albums.album_uri, regex_bad_repeat, regex_good_repeat "
                      f"WHERE lower(at1.track_name) NOT SIMILAR TO good_repeat "
                      f"AND lower(at1.track_name) SIMILAR TO bad_repeat "
               
                      f"UNION SELECT source_id, album_uri, track_uri FROM tracks "
                      f"JOIN sources USING (service_id) JOIN album_tracks USING (source_id, track_uri), regex_good_short, min_track_time "
                      f"WHERE track_duration < min_seconds/60::numeric AND track_num > 1 "
                      f"AND lower(track_name) NOT SIMILAR TO good_short "
                      ),
              },
             {'name': 'track_lists',
              'sql': (f"WITH played_tracks AS "
                      f"(SELECT source_id, album_uri, track_uri, ord "
                      f"FROM albums, jsonb_array_elements_text(track_uris) WITH ORDINALITY AS elems(track_uri, ord) "
                      f"EXCEPT SELECT source_id, album_uri, track_uri, 0 FROM auto_skips) "
                      
                      f"SELECT source_id, album_uri, jsonb_agg(track_uri ORDER BY ord) AS track_list, "
                      f"SUM(track_duration) AS play_duration "
                      f"FROM played_tracks JOIN sources USING (source_id) JOIN tracks USING (service_id, track_uri) "
                      f"GROUP BY source_id, album_uri "
                      ),
              },
             {'name': 'true_album_artists',
              'sql': (f"WITH all_artists AS "
                      f"(SELECT albums.source_id, albums.album_uri, tracks.artist_uris ->> 0 AS primary_artist_uri "
                      f"FROM albums JOIN sources USING (source_id) "
                      f"JOIN tracks ON tracks.service_id = sources.service_id AND albums.track_uris ? tracks.track_uri), "
               
                      f"counted_artists AS (select source_id, album_uri, primary_artist_uri, count(primary_artist_uri) AS freq "
                      f"FROM all_artists GROUP BY source_id, album_uri, primary_artist_uri), "

                      f"primary_artists AS (select source_id, album_uri, "
                      f"jsonb_agg(primary_artist_uri order by freq DESC) AS primary_artist_uris "
                      f"FROM counted_artists GROUP BY source_id, album_uri) "
               
                      f"SELECT primary_artists.source_id, primary_artists.album_uri, "
                      f"primary_artists.primary_artist_uris ->> 0 AS artist_uri "
                      f"FROM primary_artists JOIN albums USING (source_id, album_uri) "
                      f"JOIN sources USING (source_id) JOIN services USING (service_id) "
                      f"WHERE NOT primary_artists.primary_artist_uris ? (albums.artist_uris ->> 0) "
                      f"AND NOT (albums.artist_uris ->> 0) = services.various_artist_uri "
                      ),
              },
             {'name': 'album_artists',
              'sql': (f"SELECT albums_expanded.source_id, albums_expanded.album_uri, "
                      f"string_agg(artists.artist_name, '; ' ORDER BY albums_expanded.ord) AS artist_names "
                      f"FROM (SELECT albums.source_id, albums.album_uri, arr.artist_uri, arr.ord "
                      f"FROM albums LEFT JOIN true_album_artists USING (source_id, album_uri), "
                      f"jsonb_array_elements_text(CASE WHEN artist_uri IS NULL THEN artist_uris "
                      f"ELSE jsonb_build_array(artist_uri) END) "
                      f"WITH ORDINALITY arr(artist_uri, ord)) AS albums_expanded "
                      f"JOIN sources USING (source_id) JOIN artists USING (service_id, artist_uri) "
                      f"GROUP BY albums_expanded.source_id, albums_expanded.album_uri "
                      ),
              },
             {'name': 'release_battles',
              'sql': (f"WITH battles AS "
                      f"(SELECT user_id, source_id, album_uri, "
                      f"CASE WHEN wins IS NULL THEN 0.0 "
                      f"WHEN losses IS NULL THEN 1.0 "
                      f"ELSE (jsonb_array_length(wins) / jsonb_array_length(wins || losses))::numeric END AS win_rate, "
                      f"jsonb_array_length(COALESCE(wins, '[]')) + jsonb_array_length(COALESCE(losses, '[]')) AS n_games "
                      f"FROM ownerships WHERE NOT (wins IS NULL AND losses IS NULL)), "
                      
                      f"coeff AS (SELECT 1.96 AS z), "
                      
                      f"wilson AS "
                      f"(SELECT user_id, source_id, album_uri, "
                      f"((win_rate + (z ^ 2) / (2 * n_games) - z "
                      f"* sqrt((win_rate * (1 - win_rate) + (z ^ 2) / (4 * n_games)) / n_games)) "
                      f"/ (1 + (z ^ 2) / n_games)) AS score "
                      f"FROM battles, coeff) "
                      
                      f"SELECT user_id, source_id, album_uri, "
                      f"RANK() OVER(PARTITION BY user_id, category ORDER BY score DESC) AS ranking "
                      f"FROM wilson JOIN album_categories USING (source_id, album_uri) "
                      ),
              },
             {'name': 'critic_stars',
              'sql': (f"WITH critic_lists AS (SELECT DISTINCT critic_name, list_year FROM critics), "
                      f"weights AS (SELECT critic_name, 1/count(list_year)::numeric AS scaling FROM critic_lists "
                      f"GROUP BY critic_name), "
                      
                      f"list_size AS (SELECT phrase::numeric AS max_points FROM keywords WHERE keyword = 'max_critics_points'), "
                      
                      f"star_size AS (SELECT phrase::numeric AS max_stars FROM keywords WHERE keyword = 'max_stars'), "
                      
                      f"total_points AS (SELECT album_name, artist_names, "
                      f"sum(scaling * (max_points - list_position + 1)) AS points "
                      f"FROM critics JOIN weights USING(critic_name), list_size "
                      f"GROUP BY album_name, artist_names) "
                      
                      f"SELECT album_name, artist_names, "
                      f"round((percent_rank() OVER (ORDER BY points) * (max_stars - 1)) + 1)  AS stars "
                      f"FROM total_points, star_size"
                      f";"
                      ),
              },
             {'name': 'chart_peaks',
              'sql': (f"SELECT source_id, album_uri, peak_position FROM albums JOIN album_artists USING (source_id, album_uri) "
                      f"JOIN billboard ON "
                      f"(CASE WHEN album_artists.artist_names ~* '\s+(and|&|/)\s+' "
                      f"THEN regexp_replace(regexp_replace(lower(album_artists.artist_names), "
                      f"'\s+&\s+|\s+and\s+|\s+/\s+', '; ', 'g'), ',\s*', '; ', 'g') "
                      f"ELSE album_artists.artist_names END, "
                      f"regexp_replace(lower(albums.album_name), '\s+/\s+', '/', 'g')) "
                      f"= (CASE WHEN billboard.credit_names ~* '\s+(and|&|/)\s+' "
                      f"THEN regexp_replace(regexp_replace(lower(billboard.credit_names), "
                      f"'\s+&\s+|\s+and\s+|\s+/\s+', '; ', 'g'), ',\s*', '; ', 'g') "
                      f"ELSE billboard.credit_names END, "
                      f"regexp_replace(lower(billboard.album_title), '\s+/\s+', '/', 'g')) "
                      f";"
                      ),
              },
             {'name': 'album_stars',
              'sql': (f"WITH critics_stars_expanded AS "
                      f"(SELECT album_name, jsonb_array_elements_text(artist_names) AS artist_name, "
                      f"stars FROM critic_stars) "
                      
                      f"SELECT source_id, album_uri, MIN(stars) AS stars FROM albums "
                      f"JOIN album_artists USING (source_id, album_uri) "
                      f"JOIN critics_stars_expanded ON critics_stars_expanded.album_name = albums.album_name "
                      f"AND LOWER(album_artists.artist_names) ILIKE '%' || artist_name || '%' "
                      f"GROUP BY source_id, album_uri "
                      f";"
                      ),
              },
            ]
    
    updates = [{'name': 'update_tracks',
                'sql': (f"SELECT sources.service_id, jsonb_array_elements_text(albums.track_uris) AS track_uri FROM albums "
                        f"JOIN sources USING (source_id) "
                        f"EXCEPT SELECT service_id, track_uri FROM tracks WHERE "
                        f"(track_name IS NOT NULL AND explicit IS NOT NULL) "
                        ),
                },
               {'name': 'update_recordings',
                'sql': (f"SELECT isrc FROM albums JOIN sources USING (source_id) "
                        f"JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_uris ? tracks.track_uri "
                        f"LEFT JOIN barcodes USING (upc) "
                        f"WHERE album_type = 'compilation' OR release_type = 'compilation' "
                        f"EXCEPT SELECT isrc FROM recordings "# WHERE iswc IS NOT NULL "
                        ),
                },
               {'name': 'update_works',
                'sql': (f"SELECT iswc FROM recordings WHERE iswc IS NOT NULL "
                        f"EXCEPT SELECT iswc FROM works "#"WHERE release_year IS NOT NULL "
                        ),
                },
               {'name': 'update_artists',
                'sql': (f"SELECT service_id, jsonb_array_elements_text(artist_uris) AS artist_uri FROM tracks "
                        f"UNION SELECT service_id, jsonb_array_elements_text(artist_uris) AS artist_uri FROM albums "
                        f"JOIN sources USING (source_id) "
                        f"EXCEPT SELECT service_id, artist_uri FROM artists WHERE artist_name IS NOT NULL "
                        ),
                },
               {'name': 'update_soundtracks',
                'sql': (f"SELECT service_id, track_uri FROM tracks "
                        f"JOIN sources USING (service_id) "
                        f"JOIN albums ON sources.source_id = albums.source_id AND albums.track_uris ? tracks.track_uri "
                        f"JOIN album_categories ON albums.source_id = album_categories.source_id "
                        f"AND albums.album_uri = album_categories.album_uri "
                        f"LEFT JOIN barcodes USING (upc) "
                        f"WHERE album_categories.category = 'soundtrack' AND instrumentalness IS NULL "
                        ),
                },
               {'name': 'update_barcodes',
                'sql': (f"WITH regex_soundtrack AS (SELECT '%(' || string_agg(phrase, '|') || ')%' "
                        f"AS soundtrack_words FROM keywords WHERE keyword = 'soundtrack'), "
               
                        f"upcs AS (SELECT albums.upc FROM albums, regex_soundtrack "
                        f"WHERE albums.album_type not in ('single', 'ep', 'playlist') AND albums.upc IS NOT NULL "
                        f"AND lower(albums.album_name) NOT SIMILAR TO soundtrack_words "
                        f"EXCEPT SELECT upc FROM barcodes UNION SELECT upc FROM barcodes WHERE release_type IS NULL) "
               
                        f"SELECT upcs.upc, artists.artist_name, albums.album_name, albums.release_date FROM upcs "
                        f"JOIN albums USING (upc) JOIN sources USING (source_id) "
                        f"LEFT JOIN true_album_artists USING (source_id, album_uri) "
                        f"JOIN artists ON sources.service_id = artists.service_id "
                        f"AND COALESCE(true_album_artists.artist_uri, albums.artist_uris ->> 0) = artists.artist_uri "
                        ),
                },
              ]

    materialized = [{'name': 'user_albums',
                     'sql': (f"SELECT first_name || ' ' || last_name AS user_name, "
                             f"artist_names, album_name, category, "
                             f"release_date, COALESCE(release_decades, jsonb_build_array(FLOOR(EXTRACT(YEAR FROM release_date) / 10) * 10)) AS release_decades, "
                             f"ranking, rating, peak_position, stars, "
                             f"track_list, COALESCE(play_duration, album_duration) AS play_duration, COALESCE(explicit, FALSE) AS explicit, "
                             f"service_name, source_name, "
                             f"user_id, service_id, source_id, album_uri "
                             f"FROM ownerships JOIN sources USING (source_id) JOIN users USING (user_id) "
                             f"JOIN services USING (service_id) JOIN albums USING (source_id, album_uri) "
                             f"JOIN album_artists USING (source_id, album_uri) JOIN album_categories USING (source_id, album_uri) "
                             f"LEFT JOIN release_battles USING (user_id, source_id, album_uri) "
                             f"LEFT JOIN track_lists USING (source_id, album_uri) LEFT JOIN explicit_albums USING (source_id, album_uri) "
                             f"LEFT JOIN compilations USING (source_id, album_uri) "
                             f"LEFT JOIN chart_peaks USING (source_id, album_uri) "
                             f"LEFT JOIN album_stars USING (source_id, album_uri) "
                             ),
                     },
                    ] 
    
    summary = {'name': '_remaining_updates'}
    
    ''' basic DB functions '''
    def create_primary_key(pk):
        key = 'PRIMARY KEY (' + ', '.join(pk) + ')' if pk else ''
        return key
    
    def create_foreign_key(fkt):
        if fkt:
            fk, t = fkt
            if isinstance(fk, list):
                fk1 = fk
                fk2 = fk
            elif isinstance(fk, dict):
                fk1 = fk.keys()
                fk2 = fk.values()
            key = 'FOREIGN KEY (' + ', '.join(fk1) + f') REFERENCES {t} (' + ', '.join(fk2) + ')'
        else:
            key = ''
        return key

    ''' table setup '''
    def create_tables():
        sqls = []
        for table in SQLer.tables:
            cols_add = ', '.join(f'{c} {t}' for c, t in table['columns'])
            pk_add = ', ' + SQLer.create_primary_key(table['pk']) if table.get('pk') else ''
            fk_add = ', ' + ', '.join(SQLer.create_foreign_key([fk, t]) for fk, t in table['fks']) if table.get('fks') else ''
            sql = (f'CREATE TABLE IF NOT EXISTS {table["name"]} '
                   f'({cols_add} {pk_add} {fk_add})'
                   f';'
                   )
            sqls.append(sql)
        return sqls
    
    def drop_tables():
        sqls = []
        for table in SQLer.tables[::-1]:
            sql = f'DROP TABLE IF EXISTS {table["name"]};'
            sqls.append(sql)
        return sqls

    ''' view setup '''                    
    def create_views():
        sqls = []
        for view in SQLer.views + SQLer.updates:
            sql = f'CREATE OR REPLACE VIEW {view["name"]} AS {view["sql"]};'
            sqls.append(sql)
        return sqls

    def materialize_views():
        sqls = []
        for view in SQLer.materialized:
            sql_1 = f'DROP MATERIALIZED VIEW IF EXISTS {view["name"]};'
            sql_2 = f'CREATE MATERIALIZED VIEW {view["name"]} AS {view["sql"]};'
            sqls.extend([sql_1, sql_2])
        return sqls

    def refresh_views():
        sqls = []
        for view in SQLer.materialized:
            sql = f'REFRESH MATERIALIZED VIEW {view["name"]};'
            sqls.append(sql)
        return sqls

    def drop_views():
        sqls = []
        for view in SQLer.views[::-1] + [SQLer.summary]:
            sql = f'DROP VIEW IF EXISTS {view["name"]} CASCADE;'
            sqls.append(sql)
        return sqls
               
    def summarize_views():
        unions = ' UNION '.join(f"SELECT '{uv['name']}' AS update_name, count(*) AS remaining_rows FROM {uv['name']} " for uv in SQLer.updates) 
        sql = f'CREATE OR REPLACE VIEW {SQLer.summary["name"]} AS {unions};'
        return [sql]