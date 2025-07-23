from scripts.connectors.db_manager import DatabaseManager
from config.logging_config import setup_logging

# --------
# Manual child-parent mapping script
# Finds tracks with the same artist and title but different album names like: Track_1 (Track_1 single album) - Track_1 (Full_Standart_Album)
# Maps these tracks in the dm.parent_track table for better analytics
# --------

# The main query: finds tracks with >1 variant and total_time > 45 minutes
VARIANT_QUERY = """
WITH variants AS (
    SELECT
        dt.track_title,
        dt.artist_name,
        ARRAY_AGG(dt.track_id)          AS track_ids,
        ARRAY_AGG(dt.spotify_track_uri) AS uris,
        ARRAY_AGG(dt.album_name)        AS albums,
        COUNT(DISTINCT dt.spotify_track_uri) AS variant_count
    FROM core.dim_track dt
    WHERE dt.track_title NOT ILIKE '%Version)'             -- exclude all (Taylor's Version) tracks 
      AND dt.track_title NOT ILIKE '%(From The Vault)%'     -- and (From The Vault) tracks
    GROUP BY dt.track_title, dt.artist_name
    HAVING COUNT(DISTINCT dt.spotify_track_uri) > 1
),
total_time AS (
    SELECT
        t.track_title,
        t.artist_name,
        SUM(h.sec_played) / 60.0 AS total_time_min
    FROM core.fact_tracks_history h
    JOIN core.dim_track t ON h.track_fk = t.track_id
    GROUP BY t.track_title, t.artist_name
)
SELECT
    v.track_title,
    v.artist_name,
    v.variant_count,
    t.total_time_min,
    v.track_ids,
    v.uris,
    v.albums
FROM variants v
JOIN total_time t USING (track_title, artist_name)
WHERE t.total_time_min > 45
ORDER BY t.total_time_min DESC;
"""
UPSERT_SQL = """
INSERT INTO dm.parent_tracks
  (child_track_uri, parent_track_uri, child_id, parent_id,
   artist, child_track_title, child_album_name,
   parent_track_title, parent_album_name)
VALUES %s
ON CONFLICT (child_id) DO UPDATE
  SET parent_id       = EXCLUDED.parent_id,
      parent_track_uri= EXCLUDED.parent_track_uri,
      parent_album_name = EXCLUDED.parent_album_name,
      mapped_at       = now();
"""
VARIANT_TIME_SQL = """
SELECT
    track_fk,
    SUM(sec_played) / 60.0 AS playtime_min
FROM core.fact_tracks_history
WHERE track_fk = ANY(%s)
GROUP BY track_fk;
"""

def main():
    logger = setup_logging()
    
    with DatabaseManager(logger) as db:
        rows = db.execute_query(VARIANT_QUERY, manual_fetch=True)
        print(f"Found {len(rows)} tracks")

        for idx, (
            title, artist, count, total_min,
            track_ids, uris, albums
        ) in enumerate(rows, start=1):
            
            time_rows = db.execute_query(VARIANT_TIME_SQL, (track_ids,), manual_fetch=True)
            times = {r[0]: r[1] for r in time_rows}

            print(f"{idx}. “{title}” by {artist} — {count} variants, {total_min:.1f} min total")
            for i, (tid, uri, alb) in enumerate(zip(track_ids, uris, albums), start=1):
                minutes = times.get(tid, 0.0)
                print(f"    {i}. ID={tid} | URI={uri} | {minutes:>5.1f} min | [{alb}]")
            choice = input("Select parent # (or ‘s’kip, ‘q’uit): ").strip().lower()
            if choice == 'q':
                print("Quitting early")
                break
            if choice == 's' or not choice.isdigit() or not (1 <= int(choice) <= count):
                print("skipped.\n")
                continue

            parent_idx = int(choice) - 1
            parent_id, parent_uri, parent_album = (
                track_ids[parent_idx],
                uris[parent_idx],
                albums[parent_idx]
            )

            to_upsert = []
            for tid, uri, alb in zip(track_ids, uris, albums):
                if tid == parent_id:
                    continue
                to_upsert.append((
                    uri,                 
                    parent_uri,          
                    tid,                 
                    parent_id,           
                    artist,
                    title,               
                    alb,                 
                    title,               
                    parent_album         
                ))

            if to_upsert:
                db.bulk_insert(table_name="dm.parent_tracks", columns="child_track_uri, parent_track_uri, child_id, parent_id, artist, child_track_title, child_album_name, parent_track_title, parent_album_name".split(", "), records=to_upsert)
                print(f"Saved {len(to_upsert)} mappings.\n")
            else:
                print("No child variants to save.\n")

if __name__ == "__main__":
    main()