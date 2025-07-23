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

-- albums function
create or replace function dm.top_albums(filter_year int default null, filter_month int default null, return_limit int default 100)
returns table(album varchar, artist varchar, hours_played numeric, times_played int, full_sum_streams int, full_real_streams int)
language plpgsql
as $$
    begin
        return query
            select
                coalesce(p.parent_album_name, dt.album_name) album,
                dt.artist_name album_artist,
                round(sum(sec_played) / 3600, 1) hours_played,
                count(stream_id)::int times_played,
                round(sum(percent_played) / 100)::int full_sum_streams,
                count(case when percent_played = 100 then stream_id end)::int full_real_streams
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
returns table(track varchar, artist varchar, hours_played numeric, times_played int, full_sum_streams int, full_real_streams int)
language plpgsql
as $$
    begin
        return query
            select
                coalesce(p.parent_track_title, dt.track_title) track,
                dt.artist_name track_artist,
                round(sum(sec_played) / 3600, 1) hours_played,
                count(stream_id)::int times_played,
                round(sum(percent_played) / 100)::int full_sum_streams,
                count(case when percent_played = 100 then stream_id end)::int full_real_streams
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
returns table(artist varchar, hours_played numeric, times_played int, full_sum_streams int, full_real_streams int)
language plpgsql
as $$
    begin
        return query
            select
                dt.artist_name artist,
                round(sum(sec_played) / 3600, 1) hours_played,
                count(stream_id)::int times_played,
                round(sum(percent_played) / 100)::int full_sum_streams,
                count(case when percent_played = 100 then stream_id end)::int full_real_streams
            from core.fact_tracks_history h
                join core.dim_track dt on dt.track_id = h.track_fk
                join core.dim_date dd on h.date_fk = dd.date_id
            where (filter_year is null or dd.year = filter_year)
                and (filter_month is null or (filter_year is not null and dd.month_num = filter_month))
            group by artist
            order by hours_played desc
            limit return_limit;
    end;
$$;