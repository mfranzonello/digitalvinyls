''' Database functionality '''

class SQLer:
    ## should replace a lot of the _ids with _uris
    tables = [{'name': 'services',
               'columns': [['service_id', 'serial'],
                           ['service_name', 'varchar'],
                           ['various_artist_id', 'varchar'],
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
                           ['album_uri', 'varchar'],
                           ['artist_ids', 'jsonb'],
                           ['album_name', 'varchar'],
                           ['album_type', 'varchar'],
                           ['genres', 'jsonb'],
                           ['release_date', 'timestamp'],
                           ['image_src', 'varchar'],
                           ['track_list', 'jsonb'],
                           ['album_duration', 'numeric'],
                           ['upc', 'varchar'],
                           ['release_id', 'serial']
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
                           ##['ranking', 'integer'],
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
                           ['explicit', 'boolean']
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
              ]
  
    views = [{'name': 'compilations',
              'sql': (f"WITH full_tracks AS "
                      f"(SELECT tracks.service_id, tracks.track_id, works.release_year FROM tracks "
                      f"JOIN recordings USING(isrc) JOIN works USING(iswc)) "
               
                      f"SELECT albums_expanded.source_id, albums_expanded.album_id, "
                      f"jsonb_agg(DISTINCT COALESCE(full_tracks.release_year, EXTRACT(YEAR FROM albums_expanded.release_date))) AS release_years "
                      f"FROM (SELECT albums.source_id, albums.album_id, albums.release_date, arr.track_id "
                      f"FROM albums, jsonb_array_elements_text(track_list) as arr(track_id)) AS albums_expanded "
                      f"JOIN sources USING(source_id) LEFT JOIN full_tracks USING(service_id, track_id) "
                      f"GROUP BY albums_expanded.source_id, albums_expanded.album_id "
                      f";"
                      ),
              },
             {'name': 'album_categories',
              'sql': (f"WITH regex_soundtrack AS (SELECT '%(' || string_agg(phrase, '|') || ')%' "
                      f"AS soundtrack_words FROM keywords WHERE keyword = 'soundtrack'), "
               
                      f"min_score AS (SELECT phrase::numeric AS min_instrumentalness FROM keywords WHERE keyword = 'score_instrumental'), "

                      f"min_tracks AS (SELECT phrase::numeric AS min_ep_tracks FROM keywords WHERE keyword = 'min_ep_tracks'), "
               
                      f"min_span AS (SELECT phrase::numeric AS min_release_span FROM keywords WHERE keyword = 'min_release_span'), "
               
                      f"mbid_soundtracks AS (SELECT upc, release_type FROM barcodes), "

                      f"instrumentals AS (SELECT source_id, album_id, AVG(instrumentalness) AS instrumentalness "
                      f"FROM albums JOIN sources USING(source_id) "
                      f"JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_list ? tracks.track_id "
                      f"GROUP BY source_id, album_id), "
               
                      f"release_years AS (SELECT source_id, album_id, MAX(release_year) - MIN(release_year) AS release_span FROM albums "
                      f"JOIN sources USING(source_id) JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_list ? tracks.track_id "
                      f"JOIN recordings USING(isrc) JOIN works USING(iswc) "
                      f"GROUP BY source_id, album_id) "
               
                      f"SELECT source_id, album_id, "
                      f"CASE WHEN album_type = 'single' AND jsonb_array_length(track_list) >= min_ep_tracks THEN 'ep' "
                      f"WHEN album_type in ('album', 'compilation') THEN "
                      f"(CASE WHEN LOWER(album_name) SIMILAR TO soundtrack_words OR release_type = 'soundtrack' THEN "
                      f"(CASE WHEN instrumentalness >= min_instrumentalness THEN 'score' ELSE 'soundtrack' END) "
                      f"WHEN release_type = 'compilation' AND release_span >= min_release_span THEN 'compilation' "
                      f"WHEN album_type = 'album' THEN 'studio' ELSE album_type END) ELSE album_type END AS category "
                      f"FROM albums LEFT JOIN instrumentals USING(source_id, album_id) "
                      f"LEFT JOIN release_years USING(source_id, album_id) LEFT JOIN mbid_soundtracks USING(upc), "
                      f"regex_soundtrack, min_tracks, min_span, min_score "
                      f";"
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
                      f"album_tracks AS (SELECT source_id, album_id, elem.track_id, elem.track_num "
                      f"FROM albums, jsonb_array_elements_text(track_list) WITH ORDINALITY AS elem(track_id, track_num)), "
               
                      f"album_track_list AS (SELECT source_id, album_id, track_id, track_num, track_name FROM album_tracks "
                      f"JOIN sources USING(source_id) JOIN tracks USING (service_id, track_id)) "
               
                      f"SELECT at1.source_id, at1.album_id, at1.track_id "
                      f"FROM album_track_list AS at1 JOIN album_track_list AS at2 ON "
                      f"at1.source_id = at2.source_id AND at1.album_id = at2.album_id AND "
                      f"at1.track_name LIKE at2.track_name || '%' AND at1.track_num > at2.track_num "
                      f"JOIN albums ON at1.source_id = albums.source_id AND at1.album_id = albums.album_id, regex_bad_repeat, regex_good_repeat "
                      f"WHERE lower(at1.track_name) NOT SIMILAR TO good_repeat "
                      f"AND lower(at1.track_name) SIMILAR TO bad_repeat "
               
                      f"UNION SELECT source_id, album_id, track_id FROM tracks "
                      f"JOIN sources USING(service_id) JOIN album_tracks USING (source_id, track_id), regex_good_short, min_track_time "
                      f"WHERE track_duration < min_seconds/60::numeric AND track_num > 1 "
                      f"AND lower(track_name) NOT SIMILAR TO good_short "
                      f";"
                      ),
              },
             {'name': 'true_album_artists',
              'sql': (f"WITH all_artists AS "
                      f"(SELECT albums.source_id, albums.album_id, tracks.artist_ids ->> 0 AS primary_artist_id "
                      f"FROM albums JOIN sources USING(source_id) "
                      f"JOIN tracks ON tracks.service_id = sources.service_id AND albums.track_list ? tracks.track_id), "
               
                      f"counted_artists AS (select source_id, album_id, primary_artist_id, count(primary_artist_id) AS freq "
                      f"FROM all_artists GROUP BY source_id, album_id, primary_artist_id), "

                      f"primary_artists AS (select source_id, album_id, "
                      f"jsonb_agg(primary_artist_id order by freq DESC) AS primary_artist_ids "
                      f"FROM counted_artists GROUP BY source_id, album_id) "
               
                      f"SELECT primary_artists.source_id, primary_artists.album_id, "
                      f"primary_artists.primary_artist_ids ->> 0 AS artist_id "
                      f"FROM primary_artists JOIN albums USING(source_id, album_id) "
                      f"JOIN sources USING(source_id) JOIN services USING(service_id) "
                      f"WHERE NOT primary_artists.primary_artist_ids ? (albums.artist_ids ->> 0) "
                      f"AND NOT (albums.artist_ids ->> 0) = services.various_artist_id "
                      f";"
                      ),
              },
             {'name': 'album_artists',
              'sql': (f"SELECT albums_expanded.source_id, albums_expanded.album_id, "
                      f"string_agg(artists.artist_name, '; ' ORDER BY albums_expanded.ord) AS artist_names "
                      f"FROM (SELECT albums.source_id, albums.album_id, arr.artist_id, arr.ord "
                      f"FROM albums LEFT JOIN true_album_artists USING(source_id, album_id), "
                      f"jsonb_array_elements_text(CASE WHEN artist_id IS NULL THEN artist_ids "
                      f"ELSE jsonb_build_array(artist_id) END) "
                      f"WITH ORDINALITY arr(artist_id, ord)) AS albums_expanded "
                      f"JOIN sources USING(source_id) JOIN artists USING(service_id, artist_id) "
                      f"GROUP BY albums_expanded.source_id, albums_expanded.album_id "
                      f";"
                      ),
              },
             {'name': 'release_battles',
              'sql': (f"WITH battles AS "
                      f"(SELECT user_id, source_id, album_id, "
                      f"CASE WHEN wins IS NULL THEN 0.0 "
                      f"WHEN losses IS NULL THEN 1.0 "
                      f"ELSE (jsonb_array_length(wins) / jsonb_array_length(wins || losses))::numeric END AS win_rate, "
                      f"jsonb_array_length(COALESCE(wins, '[]')) + jsonb_array_length(COALESCE(losses, '[]')) AS n_games "
                      f"FROM ownerships WHERE NOT (wins IS NULL AND losses IS NULL)), "
                      
                      f"coeff AS (SELECT 1.96 AS z), "
                      
                      f"wilson AS "
                      f"(SELECT user_id, source_id, album_id, "
                      f"((win_rate + (z ^ 2) / (2 * n_games) - z "
                      f"* sqrt((win_rate * (1 - win_rate) + (z ^ 2) / (4 * n_games)) / n_games)) "
                      f"/ (1 + (z ^ 2) / n_games)) AS score "
                      f"FROM battles, coeff) "
                      
                      f"SELECT user_id, source_id, album_id, "
                      f"RANK() OVER(PARTITION BY user_id, category ORDER BY score DESC) AS ranking "
                      f"FROM wilson JOIN album_categories USING(source_id, album_id) "
                      f";"
                      ),
              },
             # # {'name': 'albums_to_rank',
             # #  'sql': (f"SELECT user_id, source_id, album_id, artist_name, album_name, "
             # #          f"category, ranking, rating FROM ownerships "
             # #          f"JOIN albums using(source_id, album_id) JOIN artists ON albums.artist_ids ->> 0 = artists.artist_id "
             # #          f"JOIN album_categories USING(source_id, album_id) "
             # #          f";"
             # #          ),
             # #  },
             {'name': 'user_albums',
              'sql': (f"WITH skip_tracks AS (SELECT source_id, album_id, jsonb_agg(track_id) AS skip_list "
                      f"FROM auto_skips GROUP BY source_id, album_id) "
                      
                      f"SELECT artist_names, album_name,  release_date, "
                      f"category, album_duration, release_years, source_id, album_id, artist_ids, "
                      f"track_list, skip_list, user_id, first_name || ' ' || last_name AS user_name, "
                      f"like_date, rating, ranking FROM albums "
                      f"JOIN album_artists USING(source_id, album_id) "
                      f"LEFT JOIN skip_tracks USING(source_id, album_id) "
                      f"JOIN sources USING(source_id) JOIN album_categories USING(source_id, album_id) "
                      f"JOIN compilations USING(source_id, album_id) JOIN ownerships USING(source_id, album_id) "
                      f"JOIN release_battles USING(user_id, source_id, album_id) "
                      f"JOIN users USING(user_id) ORDER BY release_date DESC "
                      f";"
                      ),
              },
             {'name': 'update_tracks',
              'sql': (f"SELECT sources.service_id, jsonb_array_elements_text(albums.track_list) AS track_id FROM albums "
                      f"JOIN sources USING(source_id) "
                      f"EXCEPT SELECT service_id, track_id FROM tracks WHERE "
                      f"(track_name IS NOT NULL AND explicit IS NOT NULL) "
                      f";"
                      ),
              },
             {'name': 'update_recordings',
              'sql': (f"SELECT tracks.isrc FROM albums JOIN sources ON albums.source_id = sources.source_id "
                      f"JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_list ? tracks.track_id "
                      f"LEFT JOIN barcodes USING(upc) "
                      f"WHERE albums.album_type = 'compilation' OR barcodes.release_type = 'compilation' "
                      f"EXCEPT SELECT isrc FROM recordings "# WHERE iswc IS NOT NULL "
                      ";"
                      ),
              },
             {'name': 'update_works',
              'sql': (f"SELECT iswc FROM recordings WHERE iswc IS NOT NULL "
                      f"EXCEPT SELECT iswc FROM works "#"WHERE release_year IS NOT NULL "
                      f";"
                      ),
              },
             {'name': 'update_artists',
              'sql': (f"SELECT service_id, jsonb_array_elements_text(artist_ids) AS artist_id FROM tracks "
                      f"UNION SELECT sources.service_id, jsonb_array_elements_text(albums.artist_ids) AS artist_id FROM albums "
                      f"JOIN sources ON albums.source_id = sources.source_id "
                      f"EXCEPT SELECT service_id, artist_id FROM artists WHERE artist_name IS NOT NULL "
                      f";"
                      ),
              },
             {'name': 'update_soundtracks',
              'sql': (f"SELECT tracks.service_id, tracks.track_id FROM tracks "
                      f"JOIN sources USING(service_id) "
                      f"JOIN albums ON sources.source_id = albums.source_id AND albums.track_list ? tracks.track_id "
                      f"JOIN album_categories ON albums.source_id = album_categories.source_id "
                      f"AND albums.album_id = album_categories.album_id "
                      f"LEFT JOIN barcodes USING(upc) "
                      f"WHERE album_categories.category = 'soundtrack' AND instrumentalness IS NULL "
                      f";"
                      ),
              },
             {'name': 'update_barcodes',
              'sql': (f"WITH regex_soundtrack AS (SELECT '%(' || string_agg(phrase, '|') || ')%' "
                      f"AS soundtrack_words FROM keywords WHERE keyword = 'soundtrack'), "
               
                      f"upcs AS (SELECT albums.upc FROM albums, regex_soundtrack "
                      f"WHERE albums.album_type not in ('single', 'ep', 'playlist') AND albums.upc IS NOT NULL "
                      f"AND LOWER(albums.album_name) NOT SIMILAR TO soundtrack_words "
                      f"EXCEPT SELECT upc FROM barcodes UNION SELECT upc FROM barcodes WHERE release_type IS NULL) "
               
                      f"SELECT upcs.upc, artists.artist_name, albums.album_name, albums.release_date FROM upcs "
                      f"JOIN albums USING(upc) JOIN sources USING(source_id) "
                      f"LEFT JOIN true_album_artists USING(source_id, album_id) "
                      f"JOIN artists ON sources.service_id = artists.service_id "
                      f"AND COALESCE(true_album_artists.artist_id, albums.artist_ids ->> 0) = artists.artist_id "
                      f";"
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
            pk_add = ', ' + SQLer.create_primary_key(table['pk']) if table['pk'] else ''
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
        for view in SQLer.views:
            sql = f'CREATE OR REPLACE VIEW {view["name"]} AS {view["sql"]}'
            sqls.append(sql)
        return sqls

    def drop_views():
        sqls = []
        for view in SQLer.views[::-1] + [SQLer.summary]:
            sql = f'DROP VIEW IF EXISTS {view["name"]} CASCADE;'
            sqls.append(sql)
        return sqls
               
    def summarize_views():
        update_views = [view['name'] for view in SQLer.views if 'update_' in view['name']]
        unions = ' UNION '.join(f"SELECT '{uv}' AS update_name, count(*) AS remaining_rows FROM {uv} " for uv in update_views) 
        sqls = [f'CREATE OR REPLACE VIEW {SQLer.summary["name"]} AS {unions};']
        return sqls