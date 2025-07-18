CREATE SCHEMA IF NOT EXISTS dm;

create table if not exists dm.parent_tracks
(
    child_track_uri    varchar not null,
    parent_track_uri   varchar,
    child_id           int,
    parent_id          int,
    artist             varchar,
    child_track_title  varchar,
    child_album_name   varchar,
    parent_track_title varchar,
    parent_album_name  varchar,
    mapped_at          timestamp default now(),
    primary key (child_id),
    foreign key (child_id) references core.dim_track(track_id),
    foreign key (parent_id) references core.dim_track(track_id)
);