''' Database tables and views '''

class SQLer:
    tables = [{'name': 'services',
               'sql': ('''
                       service_id serial,
                       service_name varchar,
                       various_artist_uri varchar,
                       explicits boolean,
                       audio_analysis boolean,
                       PRIMARY KEY (service_id)
                       '''),
               },
              {'name': 'sources',
               'sql': ('''
                       source_id serial,
                       serivce_id integer,
                       source_name varchar,
                       PRIMARY KEY (source_id),
                       FOREIGN KEY (service_id) REFERENCES services (service_id),
                       UNIQUE (service_id, service_source)
                       ''')
               }, 
              {'name': 'profiles',
               'sql': ('''
                       user_id serial,
                       first_name varchar,
                       last_name varchar,
                       user_email varchar,
                       PRIMARY KEY user_id,
                       UNIQUE user_email
                       '''),
               },        
              {'name': 'artists',
               'sql': ('''
                       service_id integer,
                       artist_uri varchar,
                       artist_name varchar,
                       PRIMARY KEY (service_id, artist_uri),
                       FOREIGN KEY (service_id) REFERENCES services (service_id)
                       ''')
               },
              {'name': 'albums',
               'sql': ('''
                       source_id integer,
                       album_uri varchar,
                       artist_uris jsonb,
                       album_name varchar,
                       album_type varchar,
                       genres jsonb,
                       release_date timestamp,
                       image_src varchar,
                       track_uris jsonb,
                       album_duration numeric,
                       upc varchar,
                       mb_id varchar,
                       lastfm_id varchar,
                       PRIMARY KEY (source_id, album_uri),
                       FOREIGN KEY (source_id) REFERENCES sources (source_id)
                       '''),
               },
              {'name': 'ownerships',
               'sql': ('''
                       user_id integer,
                       source_id integer,
                       album_uri varchar,
                       like_date timestamp,
                       rating integer,
                       wins jsonb,
                       losses json,
                       PRIMARY KEY (user_id, source_id, album_uri),
                       FOREIGN KEY (user_id) REFERENCES profiles (user_id),
                       FOREIGN KEY (source_id, album_uri) REFERENCES albums (source_id, album_uri)
                       '''),
               },
              {'name': 'tracks',
               'sql': ('''
                       serivce_id integer,
                       track_uri varchar,
                       track_name varchar,
                       track_duration numeric,
                       artist_uris jsonb,
                       isrc varchar,
                       explicit boolean,
                       PRIMARY KEY (service_id, track_uri),
                       FOREIGN KEY (service_id) REFERENCES services (service_id)
                       '''),
               },
              {'name': 'series',
               'sql': ('''
                       user_id integer,
                       series_name varchar,
                       album_list jsonb,
                       PRIMARY KEY (user_id, series_name),
                       FOREIGN KEY (user_id) REFERENCES profiles (user_id)
                       '''),
               },
              {'name': 'replacements',
               'sql': ('''
                       source_id integer,
                       album_uri varchar,
                       replace_source_id integer,
                       replace_album_uri varchar,
                       PRIMARY KEY (source_id, album_uri),
                       FOREIGN KEY (source_id, album_uri) REFERENCES albums (source_id, album_uri),
                       FOREIGN KEY (replace_source_id, replace_album_uri) REFERENCES albums (source_id, album_uri)
                       '''),
               },
              {'name': 'barcodes',
               'sql': ('''
                       barcode_id serial,
                       upc varchar,
                       release_type varchar,
                       PRIMARY KEY (barcode_id),
                       FOREIGN KEY (upc) REFERENCES albums (upc),
                       UNIQUE upc
                       '''),
               },
              {'name': 'recordings',
               'sql': ('''
                       isrc varchar,
                       iswc varchar,
                       PRIMARY KEY isrc
                       '''),
               },
              {'name': 'works',
               'sql': ('''
                       iswc varchar,
                       release_year date,
                       PRIMARY KEY iswc
                       '''),
               },
              {'name': 'lastfm',
               'sql': ('''
                       source_id integer,
                       album_uri varchar,
                       release_type varchar,
                       PRIMARY KEY(source_id, album_uri),
                       FOREIGN KEY (source_id, album_uri) REFERENCES albums (source_id, album_uri)
                       '''),
               },
              {'name': 'billboard',
               'sql': ('''
                       album_title varchar,
                       credit_names varchar,
                       peak_position integer,
                       PRIMARY KEEY (album_tile, credit_names)
                       '''),
               },
              {'name': 'critics',
               'sql': ('''
                       critic_name VARCHAR,
                       list_year INTEGER,
                       list_position INTEGER,
                       album_name VARCHAR,
                       artist_names jsonb,
                       PRIMARY KEY (critic_name, list_year, list_position)
                       ''')
               },
              {'name': '_data_updates',
               'sql': ('''
                       table_name VARCHAR,
                       start_date DATE,
                       end_date DATE
                       ''')
               },
              ]
  
    views = [{'name': 'compilations',
              'sql': ('''
                      WITH full_tracks AS 
                      (SELECT service_id, track_uri, release_year FROM tracks 
                      JOIN recordings USING (isrc) JOIN works USING (iswc)), 
                      
                      albums_expanded AS 
                      (SELECT source_id, album_uri, release_date, arr.track_uri FROM albums, 
                      jsonb_array_elements_text(track_uris) as arr(track_uri)) 
                      
                      SELECT source_id, album_uri, 
                      jsonb_agg(DISTINCT COALESCE(release_year, EXTRACT(YEAR FROM release_date))) AS release_years, 
                      jsonb_agg(DISTINCT COALESCE(floor(release_year / 10) * 10, 
                      floor(EXTRACT(YEAR FROM release_date) / 10) * 10)) AS release_decades 
                      FROM albums_expanded 
                      JOIN sources USING (source_id) LEFT JOIN full_tracks USING (service_id, track_uri) 
                      GROUP BY source_id, album_uri 
                      '''),
              },
             {'name': 'explicit_albums',
              'sql': ('''
                      SELECT source_id, album_uri, BOOL_OR(explicit) AS explicit 
                      FROM albums JOIN sources USING (source_id) 
                      JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_uris ? tracks.track_uri 
                      GROUP BY source_id, album_uri HAVING BOOL_OR(explicit) = TRUE 
                      '''),
              },
             {'name': 'album_categories',
              'sql': ('''
                      WITH regex_soundtrack AS (SELECT '%(' || string_agg(phrase, '|') || ')%' 
                      AS soundtrack_words FROM keywords WHERE keyword = 'soundtrack'), 
               
                      min_score AS (SELECT phrase::numeric AS min_instrumentalness FROM keywords WHERE keyword = 'score_instrumental'), 

                      min_tracks AS (SELECT phrase::numeric AS min_ep_tracks FROM keywords WHERE keyword = 'min_ep_tracks'), 
               
                      min_span AS (SELECT phrase::numeric AS min_release_span FROM keywords WHERE keyword = 'min_release_span'), 
               
                      mbid_soundtracks AS (SELECT upc, release_type FROM barcodes), 

                      instrumentals AS (SELECT source_id, album_uri, AVG(instrumentalness) AS instrumentalness 
                      FROM albums JOIN sources USING (source_id) 
                      JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_uris ? tracks.track_uri 
                      GROUP BY source_id, album_uri), 
               
                      release_years AS (SELECT source_id, album_uri, max(release_year) - min(release_year) AS release_span FROM albums 
                      JOIN sources USING (source_id) JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_uris ? tracks.track_uri 
                      JOIN recordings USING (isrc) JOIN works USING (iswc) 
                      GROUP BY source_id, album_uri) 
               
                      SELECT source_id, album_uri, 
                      CASE WHEN album_type = 'single' AND jsonb_array_length(track_uris) >= min_ep_tracks THEN 'ep' 
                      WHEN album_type in ('album', 'compilation') THEN 
                      (CASE WHEN lower(album_name) SIMILAR TO soundtrack_words OR release_type = 'soundtrack' THEN 
                      (CASE WHEN instrumentalness >= min_instrumentalness THEN 'score' ELSE 'soundtrack' END) 
                      WHEN release_type = 'compilation' AND release_span >= min_release_span THEN 'compilation' 
                      WHEN album_type = 'album' THEN 'studio' ELSE album_type END) ELSE album_type END AS category 
                      FROM albums LEFT JOIN instrumentals USING (source_id, album_uri) 
                      LEFT JOIN release_years USING (source_id, album_uri) LEFT JOIN mbid_soundtracks USING (upc), 
                      regex_soundtrack, min_tracks, min_span, min_score 
                      '''),
              },
             {'name': 'auto_skips',
              'sql': ('''
                      WITH regex_good_repeat AS (SELECT '%(' || string_agg(phrase, '|') || ')%' 
                      AS good_repeat FROM keywords WHERE keyword = 'good_repeat'), 
                      regex_bad_repeat AS (SELECT '%(' || string_agg(phrase, '|') || ')%' 
                      AS bad_repeat FROM keywords WHERE keyword = 'bad_repeat'), 
                      regex_good_short AS (SELECT '%(' || string_agg(phrase, '|') || ')%' 
                      AS good_short FROM keywords WHERE keyword = 'good_short'), 
                      min_track_time AS (SELECT phrase::numeric AS min_seconds FROM keywords WHERE keyword = 'min_track_time'), 
                      album_tracks AS (SELECT source_id, album_uri, elem.track_uri, elem.track_num 
                      FROM albums, jsonb_array_elements_text(track_uris) WITH ORDINALITY AS elem(track_uri, track_num)), 
               
                      album_track_uris AS (SELECT source_id, album_uri, track_uri, track_num, track_name FROM album_tracks 
                      JOIN sources USING (source_id) JOIN tracks USING (service_id, track_uri)) 
               
                      SELECT at1.source_id, at1.album_uri, at1.track_uri 
                      FROM album_track_uris AS at1 JOIN album_track_uris AS at2 ON 
                      at1.source_id = at2.source_id AND at1.album_uri = at2.album_uri AND 
                      at1.track_name LIKE at2.track_name || '%' AND at1.track_num > at2.track_num 
                      JOIN albums ON at1.source_id = albums.source_id AND at1.album_uri = albums.album_uri, regex_bad_repeat, regex_good_repeat 
                      WHERE lower(at1.track_name) NOT SIMILAR TO good_repeat 
                      AND lower(at1.track_name) SIMILAR TO bad_repeat 
               
                      UNION SELECT source_id, album_uri, track_uri FROM tracks 
                      JOIN sources USING (service_id) JOIN album_tracks USING (source_id, track_uri), regex_good_short, min_track_time 
                      WHERE track_duration < min_seconds/60::numeric AND track_num > 1 
                      AND lower(track_name) NOT SIMILAR TO good_short 
                      '''),
              },
             {'name': 'track_lists',
              'sql': ('''
                      WITH played_tracks AS 
                      (SELECT source_id, album_uri, track_uri, ord 
                      FROM albums, jsonb_array_elements_text(track_uris) WITH ORDINALITY AS elems(track_uri, ord) 
                      EXCEPT SELECT source_id, album_uri, track_uri, 0 FROM auto_skips) 
                      
                      SELECT source_id, album_uri, jsonb_agg(track_uri ORDER BY ord) AS track_list, 
                      SUM(track_duration) AS play_duration 
                      FROM played_tracks JOIN sources USING (source_id) JOIN tracks USING (service_id, track_uri) 
                      GROUP BY source_id, album_uri 
                      '''),
              },
             {'name': 'true_album_artists',
              'sql': ('''
                      WITH all_artists AS 
                      (SELECT source_id, album_uri, primary_artist_uri, ord, 
                      jsonb_array_length(track_uris) AS num_tracks 
                      FROM albums JOIN sources USING (source_id) 
                      JOIN tracks ON tracks.service_id = sources.service_id AND albums.track_uris ? tracks.track_uri, 
                      jsonb_array_elements_text(tracks.artist_uris) WITH ORDINALITY arr(primary_artist_uri, ord)), 
                      
                      counted_artists AS (SELECT source_id, album_uri, primary_artist_uri, COUNT(primary_artist_uri) AS freq 
                      FROM all_artists WHERE ord <= 2 GROUP BY source_id, album_uri, primary_artist_uri),  # could drop the ord <= 2 requirement
                      discounted_artists AS (SELECT source_id, album_uri, primary_artist_uri, freq/jsonb_array_length(track_uris)::numeric AS pct 
                      FROM counted_artists JOIN albums USING (source_id, album_uri)), 
                      
                      primary_artists AS (select source_id, album_uri, 
                      jsonb_agg(primary_artist_uri order by pct DESC) AS primary_artist_uris 
                      FROM discounted_artists WHERE pct > 0.5  
                      GROUP BY source_id, album_uri) 
                      
                      SELECT source_id, album_uri, primary_artist_uris 
                      FROM primary_artists JOIN albums USING (source_id, album_uri) 
                      JOIN sources USING (source_id) JOIN services USING (service_id) 
                      WHERE NOT artist_uris @> primary_artist_uris 
                      '''),
              },
             {'name': 'album_artists',
              'sql': ('''
                      SELECT source_id, album_uri, 
                      jsonb_agg(artist_name ORDER BY ord) AS artist_names 
                      FROM (SELECT source_id, album_uri, artist_uri, ord 
                      FROM albums LEFT JOIN true_album_artists_2 USING (source_id, album_uri), 
                      jsonb_array_elements_text(COALESCE(primary_artist_uris, artist_uris)) 
                      WITH ORDINALITY arr(artist_uri, ord)) AS albums_expanded 
                      JOIN sources USING (source_id) JOIN artists USING (service_id, artist_uri) 
                      GROUP BY source_id, album_uri 
                      '''),
              },
             {'name': 'release_battles',
              'sql': ('''
                      WITH battles AS 
                      (SELECT user_id, source_id, album_uri, 
                      CASE WHEN wins IS NULL THEN 0.0 
                      WHEN losses IS NULL THEN 1.0 
                      ELSE (jsonb_array_length(wins) / jsonb_array_length(wins || losses))::numeric END AS win_rate, 
                      jsonb_array_length(COALESCE(wins, '[]')) + jsonb_array_length(COALESCE(losses, '[]')) AS n_games 
                      FROM ownerships WHERE NOT (wins IS NULL AND losses IS NULL)), 
                      
                      coeff AS (SELECT 1.96 AS z), 
                      
                      wilson AS 
                      (SELECT user_id, source_id, album_uri, 
                      ((win_rate + (z ^ 2) / (2 * n_games) - z 
                      * sqrt((win_rate * (1 - win_rate) + (z ^ 2) / (4 * n_games)) / n_games)) 
                      / (1 + (z ^ 2) / n_games)) AS score 
                      FROM battles, coeff) 
                      
                      SELECT user_id, source_id, album_uri, 
                      RANK() OVER(PARTITION BY user_id, category ORDER BY score DESC) AS ranking 
                      FROM wilson JOIN album_categories USING (source_id, album_uri) 
                      '''),
              },
             {'name': 'critic_stars',
              'sql': ('''
                      WITH critic_lists AS (SELECT DISTINCT critic_name, list_year FROM critics), 
                      weights AS (SELECT critic_name, 1/count(list_year)::numeric AS scaling FROM critic_lists 
                      GROUP BY critic_name), 
                      
                      list_size AS (SELECT phrase::numeric AS max_points FROM keywords WHERE keyword = 'max_critics_points'), 
                      
                      star_size AS (SELECT phrase::numeric AS max_stars FROM keywords WHERE keyword = 'max_stars'), 
                      
                      total_points AS (SELECT album_name, artist_names, 
                      sum(scaling * (max_points - list_position + 1)) AS points 
                      FROM critics JOIN weights USING(critic_name), list_size 
                      GROUP BY album_name, artist_names) 
                      
                      SELECT album_name, artist_names, 
                      round((percent_rank() OVER (ORDER BY points) * (max_stars - 1)) + 1)  AS stars 
                      FROM total_points, star_size
                      '''),
              },
             {'name': 'chart_peaks',
              'sql': ('''
                      SELECT source_id, album_uri, peak_position FROM albums JOIN album_artists USING (source_id, album_uri) 
                      JOIN billboard ON 
                      (CASE WHEN album_artists.artist_names ~* '\s+(and|&|/)\s+' 
                      THEN regexp_replace(regexp_replace(lower(album_artists.artist_names), 
                      '\s+&\s+|\s+and\s+|\s+/\s+', '; ', 'g'), ',\s*', '; ', 'g') 
                      ELSE album_artists.artist_names END, 
                      regexp_replace(lower(albums.album_name), '\s+/\s+', '/', 'g')) 
                      = (CASE WHEN billboard.credit_names ~* '\s+(and|&|/)\s+' 
                      THEN regexp_replace(regexp_replace(lower(billboard.credit_names), 
                      '\s+&\s+|\s+and\s+|\s+/\s+', '; ', 'g'), ',\s*', '; ', 'g') 
                      ELSE billboard.credit_names END, 
                      regexp_replace(lower(billboard.album_title), '\s+/\s+', '/', 'g')) 
                      '''),
              },
             {'name': 'album_stars',
              'sql': ('''
                      WITH critics_stars_expanded AS 
                      (SELECT album_name, jsonb_array_elements_text(artist_names) AS artist_name, 
                      stars FROM critic_stars) 
                      
                      SELECT source_id, album_uri, MIN(stars) AS stars FROM albums 
                      JOIN album_artists USING (source_id, album_uri) 
                      JOIN critics_stars_expanded ON critics_stars_expanded.album_name = albums.album_name 
                      AND LOWER(album_artists.artist_names) ILIKE '%' || artist_name || '%' 
                      GROUP BY source_id, album_uri 
                      '''),
              },
            ]
    
    updates = [{'name': 'update_tracks',
                'sql': ('''
                        SELECT service_id, jsonb_array_elements_text(track_uris) AS track_uri FROM albums 
                        JOIN sources USING (source_id) 
                        EXCEPT SELECT service_id, track_uri FROM tracks WHERE 
                        track_name IS NOT NULL AND 
                        (explicit IS NOT NULL OR service_id NOT IN (SELECT service_id FROM services WHERE explicits)) 
                        '''),
                },
               {'name': 'update_recordings',
                'sql': ('''
                        SELECT isrc FROM albums JOIN sources USING (source_id) 
                        JOIN tracks ON sources.service_id = tracks.service_id AND albums.track_uris ? tracks.track_uri 
                        LEFT JOIN barcodes USING (upc) 
                        WHERE album_type = 'compilation' OR release_type = 'compilation'
                        EXCEPT SELECT isrc FROM recordings 
                        '''),
                },
               {'name': 'update_works',
                'sql': ('''
                        SELECT iswc FROM recordings WHERE iswc IS NOT NULL 
                        EXCEPT SELECT iswc FROM works 
                        '''),
                },
               {'name': 'update_artists',
                'sql': ('''
                        SELECT service_id, jsonb_array_elements_text(artist_uris) AS artist_uri FROM tracks 
                        UNION SELECT service_id, jsonb_array_elements_text(artist_uris) AS artist_uri FROM albums 
                        JOIN sources USING (source_id) 
                        EXCEPT SELECT service_id, artist_uri FROM artists WHERE artist_name IS NOT NULL 
                        '''),
                },
               {'name': 'update_soundtracks',
                'sql': ('''
                        SELECT service_id, track_uri FROM tracks 
                        JOIN sources USING (service_id) 
                        JOIN albums ON sources.source_id = albums.source_id AND albums.track_uris ? tracks.track_uri 
                        JOIN album_categories ON albums.source_id = album_categories.source_id 
                        AND albums.album_uri = album_categories.album_uri 
                        LEFT JOIN barcodes USING (upc) 
                        WHERE album_categories.category = 'soundtrack' AND instrumentalness IS NULL 
                        AND service_id IN (SELECT service_id FROM services WHERE audio_analysis) 
                        '''),
                },
               {'name': 'update_upcs',
                'sql': ('''
                        SELECT source_id, album_uri, artist_names, album_name, release_date 
                        FROM albums JOIN album_artists USING (source_id, album_uri) 
                        WHERE upc IS NULL OR upc = 'false' AND album_type NOT IN ('single', 'playlist') 
                        '''),
                },
               {'name': 'update_barcodes',
                'sql': ('''
                        WITH regex_soundtrack AS (SELECT '%(' || string_agg(phrase, '|') || ')%' 
                        AS soundtrack_words FROM keywords WHERE keyword = 'soundtrack'), 
                        
                        upcs AS (SELECT upc FROM albums, regex_soundtrack 
                        WHERE album_type not in ('single', 'ep', 'playlist') AND upc IS NOT NULL AND upc != 'false'
                        AND lower(album_name) NOT SIMILAR TO soundtrack_words 
                        EXCEPT SELECT upc FROM barcodes UNION SELECT upc FROM barcodes WHERE release_type IS NULL) 
                        
                        SELECT upc, artist_names, album_name, release_date FROM upcs 
                        JOIN albums USING (upc) JOIN sources USING (source_id) 
                        JOIN album_artists USING (source_id, album_uri) 
                        '''),
                },
               {'name': 'update_lastfm',
                'sql': ('''
                        WITH non_analytics as 
                        (SELECT source_id, album_uri FROM albums JOIN sources USING (source_id)
                        WHERE album_type NOT IN ('single', 'ep', 'playlist')
                        AND service_id NOT IN (SELECT service_id FROM services WHERE audio_analysis)
                        AND (upc IS NULL OR upc NOT IN (SELECT upc FROM barcodes WHERE release_type IS NOT NULL))
                        EXCEPT SELECT source_id, album_uri FROM lastfm
                        WHERE release_type IS NOT NULL)
                        
                        SELECT source_id, album_uri, album_name, artist_names
                        FROM non_analytics JOIN albums USING (source_id, album_uri)
                        JOIN album_artists USING (source_id, album_uri)
                        ''')}
              ]
    
    oprhans = [{'name': '_orphan_albums',
                'columns': ['source_id', 'album_uri'],
                'sql': ('''
                        SELECT source_id, album_uri FROM albums 
                        EXCEPT SELECT source_id, album_uri FROM ownerships
                        '''),
                },
               {'name': '_orphan_tracks',
                'columns': ['service_id', 'track_uri'],
                'sql': ('''
                        SELECT service_id, track_uri FROM tracks
                        EXCEPT
                        SELECT service_id, jsonb_array_elements_text(track_uris) AS track_uri FROM albums
                        JOIN sources USING (source_id)
                        '''),
                },
               {'name': '_orphan_artists',
                'columns': ['service_id', 'artist_uri'],
                'sql': ('''
                        SELECT service_id, artist_uri FROM artists
                        EXCEPT SELECT service_id, jsonb_array_elements_text(artist_uris) FROM tracks
                        EXCEPT SELECT source_id, jsonb_array_elements_text(artist_uris) FROM albums
                        JOIN sources USING (source_id)
                        '''),
                },
               {'name': '_ophan_barcodes',
                'columns': ['upc'],
                'sql': ('''
                        SELECT upc FROM barcodes
                        EXCEPT SELECT upc FROM albums
                        '''),
                },
               {'name': '_ophan_recordings',
                'columns': ['isrc'],
                'sql': ('''
                        SELECT isrc FROM recordings
                        EXCEPT SELECT isrc FROM tracks
                        '''),
                },
               {'name': '_ophan_works',
                'columns': ['iswc'],
                'sql': ('''
                        SELECT iswc FROM works
                        EXCEPT SELECT iswc FROM recordings
                        '''),
                },
               ]
     
    materialized = [{'name': 'user_albums',
                     'sql': ('''
                             SELECT first_name || ' ' || last_name AS user_name, 
                             artist_names, album_name, category, 
                             release_date, COALESCE(release_decades, jsonb_build_array(FLOOR(EXTRACT(YEAR FROM release_date) / 10) * 10)) AS release_decades, 
                             ranking, rating, peak_position, stars, 
                             track_list, COALESCE(play_duration, album_duration) AS play_duration, COALESCE(explicit, FALSE) AS explicit, 
                             service_name, source_name, 
                             user_id, service_id, source_id, album_uri 
                             FROM ownerships JOIN sources USING (source_id) JOIN profiles USING (user_id) 
                             JOIN services USING (service_id) JOIN albums USING (source_id, album_uri) 
                             JOIN album_artists USING (source_id, album_uri) JOIN album_categories USING (source_id, album_uri) 
                             LEFT JOIN release_battles USING (user_id, source_id, album_uri) 
                             LEFT JOIN track_lists USING (source_id, album_uri) LEFT JOIN explicit_albums USING (source_id, album_uri) 
                             LEFT JOIN compilations USING (source_id, album_uri) 
                             LEFT JOIN chart_peaks USING (source_id, album_uri) 
                             LEFT JOIN album_stars USING (source_id, album_uri) 
                             WHERE jsonb_array_length(track_list) > 0
                             '''),
                     },
                    ] 
    
    summary = {'name': '_remaining_updates'}
    
    ''' table setup '''
    def create_tables():
        sqls = []
        for table in SQLer.tables:
            sql = f'CREATE TABLE IF NOT EXISTS {table["name"]} ({table["sql"]});'
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
        for view in SQLer.views + SQLer.updates + SQLer.oprhans:
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