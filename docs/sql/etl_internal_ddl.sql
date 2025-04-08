CREATE SCHEMA IF NOT EXISTS etl_internal;

create table if not exists etl_internal.failed_uris
(
    uri            varchar not null,
    entity_type    varchar not null,
    error_reason   text      default 'API returned null'::text,
    failed_at      timestamp default CURRENT_TIMESTAMP,
    retry_attempts integer   default 0,
    primary key (uri)
);

create table if not exists etl_internal.parent_tracks
(
    child_track_uri    varchar not null,
    parent_track_uri   varchar,
    artist             varchar,
    child_track_title  varchar,
    child_album_name   varchar,
    parent_track_title varchar,
    parent_album_name  varchar,
    primary key (child_track_uri)
);