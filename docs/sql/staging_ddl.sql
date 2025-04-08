create table staging.streaming_history
(
    ts                                timestamp with time zone not null,
    platform                          text                     not null,
    ms_played                         integer                  not null,
    conn_country                      varchar(2)               not null,
    ip_addr                           inet,
    master_metadata_track_name        text,
    master_metadata_album_artist_name text,
    master_metadata_album_album_name  text,
    spotify_track_uri                 text,
    episode_name                      text,
    episode_show_name                 text,
    spotify_episode_uri               text,
    reason_start                      text,
    reason_end                        text,
    shuffle                           boolean                  not null,
    skipped                           boolean                  not null,
    offline                           boolean                  not null,
    offline_timestamp                 bigint,
    incognito_mode                    boolean                  not null
);

create table staging.spotify_tracks_data
(
    record_id         serial,
    spotify_track_uri varchar,
    raw_data          jsonb,
    fetched_at        timestamp default CURRENT_TIMESTAMP,
    is_processed      boolean   default false
);

create table staging.spotify_episodes_data
(
    record_id           serial,
    spotify_episode_uri varchar,
    raw_data            jsonb,
    fetched_at          timestamp default CURRENT_TIMESTAMP,
    is_processed        boolean   default false
);

create table staging.spotify_artists_data
(
    record_id          serial,
    spotify_artist_uri varchar,
    raw_data           jsonb,
    fetched_at         timestamp default CURRENT_TIMESTAMP,
    is_processed       boolean   default false
);

create table staging.spotify_podcasts_data
(
    record_id           serial,
    spotify_podcast_uri varchar,
    raw_data            jsonb,
    fetched_at          timestamp default CURRENT_TIMESTAMP,
    is_processed        boolean   default false
);

