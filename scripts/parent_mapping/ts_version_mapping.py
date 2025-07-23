import sys
from scripts.connectors.db_manager import DatabaseManager
from config.logging_config import setup_logging

# All TS-version tracks not yet mapped in dm.parent_tracks
FETCH_TS_SQL = """
SELECT
    dt.track_id,
    dt.spotify_track_uri,
    dt.track_title,
    dt.album_name,
    dt.artist_name
FROM core.dim_track dt
LEFT JOIN dm.parent_tracks p ON dt.track_id = p.child_id
WHERE dt.track_title ILIKE '%(Taylor''s Version)%'
  AND p.child_id IS NULL
ORDER BY dt.artist_name, dt.track_title
"""

# Look for an existing parent suggestion by stripped title
SUGGEST_PARENT_SQL = """
SELECT DISTINCT
    parent_track_title,
    parent_album_name
FROM dm.parent_tracks
WHERE artist = %s
  AND parent_track_title = replace(%s, ' (Taylor''s Version)', '')
LIMIT 1
"""

# 3. Upsert into dm.parent_tracks
UPSERT_SQL = """
INSERT INTO dm.parent_tracks (
    child_track_uri,
    child_id,
    artist,
    child_track_title,
    child_album_name,
    parent_track_title,
    parent_album_name
)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (child_id) DO UPDATE 
  SET parent_track_title = EXCLUDED.parent_track_title,
      parent_album_name  = EXCLUDED.parent_album_name,
      mapped_at          = now()
"""


def prompt_manual(field_name):
    """Prompt user to type-in a value for the given field."""
    return input(f"   Enter {field_name!r}: ").strip()


def main(logger):

    with DatabaseManager(logger=logger) as db:
        ts_tracks = db.execute_query(FETCH_TS_SQL, ())
        if not ts_tracks:
            print("No unmapped Taylor’s Version tracks found")
            return

        print(f"\nFound {len(ts_tracks)} TS-version tracks to map.\n")

        for idx, (child_id, child_uri, title, album, artist) in enumerate(ts_tracks, 1):
            stripped = title.replace(" (Taylor's Version)", "")
            print(f"{idx}. “{title}”  [{album}]  by {artist}")

            # see if there's an existing suggestion
            suggestion = db.execute_query(SUGGEST_PARENT_SQL, (artist, title))
            if suggestion:
                parent_title, parent_album = suggestion[0]
                print(f"Suggested parent → “{parent_title}”  [{parent_album}]")
                print("Options:")
                print("1) Use suggested (both title+album)")
                print("2) Enter parent TRACK title only")
                print("3) Enter parent ALBUM name only")
                print("4) Enter both manually")
                print("s) Skip  q) Quit")

                choice = input("- Select option: ").strip().lower()
                if choice == 'q':
                    print("Quitting"); return
                if choice == 's':
                    print("- skipped.\n"); continue

                if choice == '1':
                    final_title, final_album = parent_title, parent_album
                elif choice == '2':
                    final_title = prompt_manual("parent TRACK title")
                    final_album = parent_album
                elif choice == '3':
                    final_title = parent_title
                    final_album = prompt_manual("parent ALBUM name")
                elif choice == '4':
                    final_title = prompt_manual("parent TRACK title")
                    final_album = prompt_manual("parent ALBUM name")
                else:
                    print("- invalid choice, skipped\n"); continue

            else:
                # no suggestion — must enter both
                print("No suggestion found.")
                final_title = prompt_manual("parent TRACK title")
                final_album = prompt_manual("parent ALBUM name")

            # upsert mapping
            params = (
                child_uri,
                child_id,
                artist,
                title,
                album,
                final_title,
                final_album
            )
            db.execute_query(UPSERT_SQL, params)
            print(f"Mapped ->  “{final_title}”  [{final_album}]\n")

    print("All TS tracks processed")

if __name__ == "__main__":
    logger = setup_logging()
    logger.info("Started mapping TS versions")
    try:
        main(logger)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(0)
    finally:
        logger.info("Finished mapping TS versions")
