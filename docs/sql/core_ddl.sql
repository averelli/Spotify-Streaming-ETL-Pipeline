create schema if not exists core;

create table if not exists core.dim_track
(
    track_id           serial,
    spotify_track_uri  varchar,
    track_title        text,
    cover_art_url      text,
    album_name         text,
    album_spotify_id   varchar,
    album_type         varchar,
    artist_name        varchar,
    spotify_artist_uri varchar,
    release_date       date,
    duration_ms        integer,
    duration_sec       integer,
    parent_track_id    integer,
    primary key (track_id),
    constraint unique_track_uri
        unique (spotify_track_uri)
);

create table if not exists core.dim_artist
(
    artist_id          serial,
    spotify_artist_uri varchar,
    cover_art_url      text,
    artist_name        text,
    primary key (artist_id)
);

create table if not exists core.dim_reason
(
    reason_id    serial,
    reason_type  varchar,
    reason_group varchar(5),
    primary key (reason_id),
    constraint unique_reason_pair unique (reason_type, reason_group)
);

create table if not exists core.dim_date
(
    date_id    integer not null,
    date       date,
    year       smallint,
    month_num  smallint,
    month_abbr varchar(3),
    month_name varchar(9),
    day        smallint,
    day_name   varchar,
    week       smallint,
    is_weekend boolean,
    primary key (date_id)
);

create table if not exists core.dim_time
(
    time_id     integer not null,
    time        time,
    hour        smallint,
    minute      smallint,
    part_of_day text,
    primary key (time_id)
);

create table if not exists core.dim_episode
(
    episode_id          serial,
    spotify_episode_uri varchar,
    duration_ms         integer,
    duration_sec        integer,
    podcast_name        varchar,
    spotify_podcast_uri varchar,
    release_date        date,
    primary key (episode_id)
);

create table if not exists core.dim_podcast
(
    podcast_id            serial,
    spotify_podcast_uri   varchar,
    podcast_name          varchar,
    description           text,
    podcast_cover_art_url varchar,
    primary key (podcast_id)
);

create table if not exists core.fact_podcasts_history
(
    stream_id       serial,
    ts_msk          timestamp,
    date_fk         integer,
    time_fk         integer,
    sec_played      integer,
    episode_fk      integer,
    podcast_fk      integer,
    reason_start_fk integer,
    reason_end_fk   integer,
    primary key (stream_id),
    foreign key (date_fk) references core.dim_date,
    foreign key (time_fk) references core.dim_time,
    foreign key (reason_start_fk) references core.dim_reason,
    foreign key (reason_end_fk) references core.dim_reason,
    foreign key (episode_fk) references core.dim_episode,
    constraint fact_podcasts_history_show_fk_fkey
        foreign key (podcast_fk) references core.dim_podcast
);

create table if not exists core.fact_tracks_history
(
    stream_id               serial,
    ts_msk                  timestamp,
    date_fk                 integer,
    time_fk                 integer,
    ms_played               integer,
    sec_played              integer,
    track_fk                integer,
    artist_fk               integer,
    reason_start_fk         integer,
    reason_end_fk           integer,
    shuffle                 boolean,
    percent_played          float,
    offline                 boolean,
    offline_timestamp       bigint,
    primary key (stream_id),
    foreign key (date_fk) references core.dim_date,
    foreign key (time_fk) references core.dim_time,
    foreign key (track_fk) references core.dim_track,
    foreign key (artist_fk) references core.dim_artist,
    foreign key (reason_start_fk) references core.dim_reason,
    foreign key (reason_end_fk) references core.dim_reason
);