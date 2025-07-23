-- This script is only for post manual mapping just to map album names without changing track names, so Style from 1989(delux) maps to Style 1989

-- 1989
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    '1989'
from core.dim_track
where album_name like '1989%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> '1989';

-- Fearless
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'Fearless'
from core.dim_track
where album_name like 'Fearless%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'Fearless' and artist_name ilike '%taylor%';

--Speak Now
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'Speak Now'
from core.dim_track
where album_name like 'Speak Now%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'Speak Now' and artist_name ilike '%taylor%';

--folklore
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'folklore'
from core.dim_track
where album_name like 'folklore%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'folklore' and artist_name ilike '%taylor%';

-- Unreal Unearth: Unending
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'Unreal Unearth: Unending'
from core.dim_track
where album_name ilike 'unreal%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'Unreal Unearth: Unending' and artist_name ilike '%hozier%';

-- brat
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'BRAT'
from core.dim_track
where album_name ilike 'brat%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'BRAT' and artist_name ilike '%xcx%';

-- dua lipa
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'Radical Optimism'
from core.dim_track
where album_name ilike 'Radical Optimism%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'Radical Optimism' and artist_name ilike '%dua%';

-- Wasteland, Baby!
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'Wasteland, Baby!'
from core.dim_track
where album_name ilike 'Wasteland%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'Wasteland, Baby!' and artist_name ilike '%hozier%';

--Midnights
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'Midnights'
from core.dim_track
where album_name like 'Midnights%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'Midnights' and artist_name ilike '%taylor%';

-- Red
insert into dm.parent_tracks (child_track_uri, child_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name)
select
    spotify_track_uri,
    track_id,
    artist_name,
    track_title,
    album_name,
    track_title,
    'folklore'
from core.dim_track
where album_name like 'the lakes (original version)%' and track_id not in (select child_id from dm.parent_tracks) and album_name <> 'folklore' and artist_name ilike '%taylor%';
