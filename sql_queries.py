# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
                            CREATE TABLE IF NOT EXISTS songplays (
                                songplay_id SERIAL PRIMARY KEY, 
                                start_time timestamp, 
                                user_id varchar NOT NULL, 
                                level varchar, 
                                song_id varchar NOT NULL, 
                                artist_id varchar NOT NULL, 
                                session_id int, 
                                location varchar, 
                                user_agent varchar
                                );
""")

create_tmp_songplays_table = ("""
                            CREATE TEMP TABLE tmp_songplays (
                                ts TIMESTAMP, 
                                userId VARCHAR, 
                                level VARCHAR, 
                                sessionId VARCHAR, 
                                location VARCHAR, 
                                userAgent VARCHAR, 
                                song VARCHAR, 
                                artist VARCHAR, 
                                length NUMERIC) 
                            ON COMMIT DROP;
""")

user_table_create = ("""
                        CREATE TABLE IF NOT EXISTS users (
                            user_id int NOT NULL, 
                            first_name varchar, 
                            last_name varchar, 
                            gender varchar, 
                            level varchar
                            );
""")

song_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songs (
                        song_id varchar NOT NULL, 
                        title varchar, 
                        artist_id varchar, 
                        year int, 
                        duration numeric
                        );
""")

artist_table_create = ("""
                            CREATE TABLE IF NOT EXISTS artists (
                                artist_id varchar NOT NULL, 
                                name varchar, 
                                location varchar, 
                                latitude varchar, 
                                longitude varchar
                                );
""")

time_table_create = ("""
                        CREATE TABLE IF NOT EXISTS time (
                        start_time timestamp, 
                        hour int, 
                        day int, 
                        week int, 
                        month int, 
                        year int, 
                        weekday int
                        );
""")


# INSERT RECORDS

songplay_table_insert = ("""
                            INSERT INTO songplays (
                            start_time, 
                            user_id, 
                            level, 
                            song_id, 
                            artist_id, 
                            session_id, 
                            location, 
                            user_agent) 
                            
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s) 
                            ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""
                        INSERT INTO users (
                        user_id, 
                        first_name, 
                        last_name, 
                        gender, 
                        level) 
                        VALUES (%s,%s,%s,%s,%s);
""")

song_table_insert = ("""
                        INSERT INTO songs (
                        song_id, 
                        title, 
                        artist_id, 
                        year, 
                        duration) 
                        VALUES (%s,%s,%s,%s,%s);
""")

artist_table_insert = ("""
                            INSERT INTO artists (
                            artist_id, 
                            name, 
                            location, 
                            latitude, 
                            longitude) 
                            VALUES (%s,%s,%s,%s,%s);
""")

songplays_table_bulk_insert = """
                        INSERT INTO songplays (
                                start_time, 
                                user_id, 
                                level, 
                                song_id, 
                                artist_id, 
                                session_id, 
                                location, 
                                user_agent) 
                            (SELECT 
                                tmp_songplays.ts, 
                                tmp_songplays.userId, 
                                tmp_songplays.level, 
                                songs.song_id, 
                                artists.artist_id, 
                                tmp_songplays.sessionId, 
                                tmp_songplays.location, 
                                tmp_songplays.userAgent 
                            FROM tmp_songplays 
                            LEFT JOIN songs ON tmp_songplays.song = songs.title 
                                AND tmp_songplays.length = songs.duration
                            LEFT JOIN artists ON tmp_songplays.artist = artists.name
                            WHERE songs.song_id IS NOT NULL AND artists.artist_id IS NOT NULL)
                            ON CONFLICT DO NOTHING;
                            DROP TABLE IF EXISTS tmp_songplays;
"""

time_table_insert = ("""
                        INSERT INTO time (
                            start_time, 
                            hour, 
                            day, 
                            week, 
                            month, 
                            year, 
                            weekday) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s);
""")

# FIND SONGS
song_select = ("""
                    SELECT songs.song_id, songs.artist_id 
                    FROM (songs JOIN artists ON songs.artist_id = artists.artist_id);
""")

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]