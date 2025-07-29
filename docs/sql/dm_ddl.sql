CREATE SCHEMA IF NOT EXISTS dm;

create table if not exists dm.parent_tracks
(
    child_track_uri    varchar not null,
    child_id           integer not null,
    artist             varchar,
    child_track_title  varchar,
    child_album_name   varchar,
    parent_track_title varchar,
    parent_album_name  varchar,
    mapped_at          timestamp default now(),
    primary key (child_id),
    foreign key (child_id) references core.dim_track
);

-- yearly aggregations
create or replace view dm.yearly_agg as
select
    year,
    make_date(year, 01, 01) year_start,
    round(sum(sec_played) / 3600.0, 1) hours_listened,
    count(stream_id) total_streams_sessions,
    count(case when sec_played > 10 then stream_id end) nonskip_sessions,
    round(sum(percent_played) / 100) total_estimated_streams,
    count(distinct track_fk) distinct_tracks,
    count(distinct artist_fk) distinct_artists
from core.fact_tracks_history fh
    join core.dim_date dd on fh.date_fk = dd.date_id
group by year
order by year desc;

-- monthly aggregations
create or replace view dm.monthly_agg as
select
    year,
    month_num,
    make_date(year, month_num, 01) month_start,
    round(sum(sec_played) / 3600.0, 1) hours_listened,
    count(stream_id) total_streams,
    count(case when sec_played > 10 then stream_id end) nonskip_streams,
    round(sum(percent_played) / 100) total_estimated_streams,
    count(distinct track_fk) distinct_tracks,
    count(distinct artist_fk) distinct_artists
from core.fact_tracks_history fh
    join core.dim_date dd on fh.date_fk = dd.date_id
group by year, month_num
order by year desc, month_num desc;

-- albums function
create or replace function dm.top_albums(filter_year int default null, filter_month int default null, return_limit int default 100)
returns table(album varchar, artist varchar, hours_played numeric, raw_play_count int, estimated_full_streams int, full_real_streams int, cover_art text)
language plpgsql
as $$
    begin
        return query
            select
                coalesce(p.parent_album_name, dt.album_name) album,
                dt.artist_name album_artist,
                round(sum(sec_played) / 3600.0, 1) hours_played,
                count(stream_id)::int times_played,
                round(sum(percent_played) / 100)::int estimated_streams,
                count(case when percent_played = 100 then stream_id end)::int full_real_streams,
                max(dt.cover_art_url) cover_art -- since we don't have an album dim random track cover art should be good enough
            from core.fact_tracks_history h
                join core.dim_track dt on dt.track_id = h.track_fk
                join core.dim_date dd on h.date_fk = dd.date_id
                left join dm.parent_tracks p on h.track_fk = p.child_id
            where (filter_year is null or dd.year = filter_year)
                and (filter_month is null or (filter_year is not null and dd.month_num = filter_month))
            group by album, album_artist
            order by hours_played desc
            limit return_limit;
    end;
$$;

-- tracks function
create or replace function dm.top_tracks(filter_year int default null, filter_month int default null, return_limit int default 100)
returns table(track varchar, artist varchar, hours_played numeric, raw_play_count int, estimated_full_streams int, full_real_streams int, cover_art text)
language plpgsql
as $$
    begin
        return query
            select
                coalesce(p.parent_track_title, dt.track_title) track,
                dt.artist_name track_artist,
                round(sum(sec_played) / 3600.0, 1) hours_played,
                count(stream_id)::int times_played,
                round(sum(percent_played) / 100)::int estimated_streams,
                count(case when percent_played = 100 then stream_id end)::int full_real_streams,
                max(dt.cover_art_url) cover_art
            from core.fact_tracks_history h
                join core.dim_track dt on dt.track_id = h.track_fk
                join core.dim_date dd on h.date_fk = dd.date_id
                left join dm.parent_tracks p on h.track_fk = p.child_id
            where (filter_year is null or dd.year = filter_year)
                and (filter_month is null or (filter_year is not null and dd.month_num = filter_month))
            group by track, track_artist
            order by hours_played desc
            limit return_limit;
    end;
$$;

-- artists function
create or replace function dm.top_artists(filter_year int default null, filter_month int default null, return_limit int default 100)
returns table(artist text, hours_played numeric, raw_play_count int, estimated_full_streams int, full_real_streams int, cover_art text)
language plpgsql
as $$
    begin
        return query
            select
                da.artist_name artist,
                round(sum(sec_played) / 3600.0, 1) hours_played,
                count(stream_id)::int times_played,
                round(sum(percent_played) / 100)::int full_sum_streams,
                count(case when percent_played = 100 then stream_id end)::int full_real_streams,
                max(da.cover_art_url) cover_art
            from core.fact_tracks_history h
                join core.dim_date dd on h.date_fk = dd.date_id
                join core.dim_artist da on h.artist_fk = da.artist_id
            where (filter_year is null or dd.year = filter_year)
                and (filter_month is null or (filter_year is not null and dd.month_num = filter_month))
            group by artist
            order by hours_played desc
            limit return_limit;
    end;
$$;