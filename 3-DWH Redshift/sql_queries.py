import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stage_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INT,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR,
    userId INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stage_songs(
    song_id VARCHAR,
    num_songs INT,
    title VARCHAR,
    artist_name VARCHAR,
    artist_latitude FLOAT,
    year INT,
    duration FLOAT,
    artist_id VARCHAR,
    artist_longitude FLOAT,
    artist_location VARCHAR
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY sortkey, 
    start_time TIMESTAMP NOT NULL, 
    user_id INT NOT NULL, 
    level VARCHAR(5),
    song_id VARCHAR(20) NOT NULL,
    artist_id VARCHAR(20) NOT NULL, 
    session_id INT, 
    location VARCHAR(50), 
    user_agent VARCHAR(140))
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY sortkey,
    first_name VARCHAR(20),
    last_name VARCHAR(20),
    gender CHAR(1),
    level VARCHAR(5))
    DISTSTYLE ALL
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(20) PRIMARY KEY sortkey distkey,
    title VARCHAR, 
    artist_id VARCHAR(20) NOT NULL, 
    year INT, 
    duration FLOAT(5))
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(20) PRIMARY KEY distkey, 
    name VARCHAR, 
    location VARCHAR, 
    latitude FLOAT(5), 
    longitude FLOAT(5))
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY distkey sortkey, 
    hour INT, 
    day INT, 
    week INT, 
    month INT, 
    year INT, 
    weekday INT)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy stage_events 
    from {events_bucket}
    credentials 'aws_iam_role={iam_role}'
    timeformat as 'epochmillisecs'
    format as json {json_path} 
    compupdate off 
    region 'us-west-2';
""").format(events_bucket=config.get('S3','LOG_DATA'), 
            iam_role=config.get('IAM_ROLE','ARN'), 
            json_path=config.get('S3','LOG_JSONPATH')
           )

staging_songs_copy = ("""
    copy stage_songs 
    from {song_bucket}
    credentials 'aws_iam_role={iam_role}'
    format as json 'auto' 
    compupdate off 
    region 'us-west-2';
""").format(song_bucket=config.get('S3','SONG_DATA'),
            iam_role=config.get('IAM_ROLE','ARN')
           )

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(e.ts,'YYYY-MM-DD HH24:MI:SS') as start_time,
                    e.userId as user_id,
                    e.level as level,
                    s.song_id as song_id,
                    s.artist_id as artist_id,
                    e.sessionId as session_id,
                    e.location as location,
                    e.userAgent as user_agent
    FROM 
        stage_events e
    JOIN 
        stage_songs s
    ON e.song = s.title
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
                    firstName as first_name,
                    lastName as last_name,
                    gender as gender,
                    level as level
    FROM 
        stage_events
    WHERE
        userid IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id as song_id,
                    title as title,
                    artist_id as artist_id,
                    year as year,
                    duration as duration
    FROM 
        stage_songs
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id as artist_id,
                    artist_name as name,
                    artist_location as location,
                    artist_latitude as latitude,
                    artist_longitude as longitude
    FROM 
        stage_songs
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT distinct ts,
                    EXTRACT(hour from ts),
                    EXTRACT(day from ts),
                    EXTRACT(week from ts),
                    EXTRACT(month from ts),
                    EXTRACT(year from ts),
                    EXTRACT(weekday from ts)
    FROM 
        stage_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
