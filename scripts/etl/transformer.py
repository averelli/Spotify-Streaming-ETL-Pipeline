from scripts.connectors.db_manager import DatabaseManager 
from psycopg2.extras import execute_values
import logging
import time

class DataTransformer:
    def __init__(self, db: DatabaseManager, logger: logging.Logger):
        self.db = db
        self.logger = logger
        
        self.BATCH_SIZE = 50

    def _clean_track(self, raw_track:dict):
        """
        Transforms a raw track JSON into a clean track tuple.
        
        Args:
            raw_track (dict): Raw JSON from staging.
        
        Returns:
            tuple: (spotify_track_uri, track_title, cover_art_url, album_name, album_spotify_id, album_type,
                    artist_name, release_date, duration_ms, duration_sec)
            or None if an error occurs.
        """
        try:
            # normalize release date based on precision
            precision = raw_track["album"]["release_date_precision"]
            release_date = raw_track["album"]["release_date"]
            
            clean_date = self._normalise_date(release_date, precision, raw_track["uri"])

            cover_art_url = raw_track["album"]["images"][0]["url"] if raw_track["album"].get("images") else None

            clean_track = (
                raw_track["uri"], 
                raw_track["name"], 
                cover_art_url, 
                raw_track["album"]["name"], 
                raw_track["album"]["id"], 
                raw_track["album"]["album_type"], 
                raw_track["artists"][0]["name"], 
                raw_track["artists"][0]["uri"],
                clean_date, 
                raw_track["duration_ms"], 
                int(round(raw_track["duration_ms"] / 1000, 0))
            )
            
            return clean_track
        
        except Exception as e:
            self.logger.error(f"Error cleaning track data for track {raw_track.get('uri')}: {e}")
            return None
    
    def _clean_artist(self, raw_artist:dict):
        """
        Transforms a raw track JSON into a clean artist tuple.
        
        Args:
            raw_artist (dict): Raw JSON from staging.
        
        Returns:
            tuple: (spotify_artist_uri, artist_image_url, artist_name) 
            or None if an error occurs.
        """
        try:
            cover_art_url = raw_artist["images"][0]["url"] if raw_artist.get("images") else None
            clean_artist = (
                raw_artist["uri"],
                cover_art_url,
                raw_artist["name"],
            )
            return clean_artist
        except Exception as e:
            self.logger.error(f"Error cleaning artist data for artist {raw_artist.get('uri')}: {e}")
            return None
        
    def _clean_podcast(self, raw_podcast:dict):
        """
        Transforms a raw podcast JSON into a clean podcast tuple.
        
        Args:
            raw_podcast (dict): Raw podcast JSON from the staging layer.
        
        Returns:
            tuple or None: (spotify_podcast_uri, show_name, description, cover_art_url),
            or None if an error occurs.
        """
        try:
            cover_art_url = raw_podcast["images"][0]["url"] if raw_podcast.get("images") else None
            clean_podcast = (
                raw_podcast["uri"],
                raw_podcast["name"],
                raw_podcast["description"],
                cover_art_url
            )
            return clean_podcast
        
        except Exception as e:
            self.logger.error(f"Error cleaning podcast data for artist {raw_podcast.get('uri')}: {e}")
            return None
        
    def _clean_episode(self, raw_episode:dict):
        """
        Transforms a raw episode JSON into a clean episode tuple.
        
        Args:
            raw_episode (dict): Raw episode JSON from the staging layer.
        
        Returns:
            tuple or None: (spotify_episode_uri, duration_ms, duration_sec, show_name, spotify_show_uri, release_date), 
            or None if an error occurs.
        """
        try:
            release_date = raw_episode["release_date"]
            precision = raw_episode["release_date_precision"]

            clean_date = self._normalise_date(release_date, precision, raw_episode["uri"])

            clean_episode = (
                raw_episode["uri"],
                raw_episode["duration_ms"],
                int(round(raw_episode["duration_ms"] / 1000, 0)),
                raw_episode["show"]["name"],
                raw_episode["show"]["uri"],
                clean_date
            )
            return clean_episode

        except Exception as e:
            return None

    def _normalise_date(self, release_date:str, precision:str, item_uri:str):
        """
        Normalizes a release date based on its precision.
        
        If the release date starts with '0000', logs a warning and returns a default date.
        For 'year' precision, appends '-01-01'; for 'month' precision, appends '-01'; otherwise, returns as is.
        
        Args:
            release_date (str): The raw release date.
            precision (str): The precision of the release date (e.g., 'year', 'month', 'day').
            item_uri (str): The URI of the item, used for logging if needed.
        
        Returns:
            str: The normalized release date.
        """
        # handle the '0000' edge case
        if release_date.startswith("0000"):
            self.logger.warning(f"Invalid release date for track {item_uri}: {release_date}. Setting as 1900-01-01.")
            clean_date = "1900-01-01"
        else:
            if precision == "year":
                clean_date = release_date + "-01-01"
            elif precision == "month":
                clean_date = release_date + "-01"
            else:
                clean_date = release_date
            
        return clean_date
        
    def process_staged_batches(self, item_type:str):
        # metrics
        time_start = time.perf_counter()
        total_items_count = 0

        self.logger.info(f"Started processing staged {item_type}")

        # based on the item type, select the cleaning function and columns list
        cleaning_func = None
        columns = []
        target_table = None

        if item_type == "tracks":
            cleaning_func = self._clean_track
            columns = ["spotify_track_uri", "track_title", "cover_art_url", "album_name", "album_spotify_id", "album_type", "artist_name", "spotify_artist_uri", "release_date", "duration_ms", "duration_sec"]
            target_table = "core.dim_track"

        elif item_type == "artists":
            cleaning_func = self._clean_artist
            columns = ["spotify_artist_uri", "cover_art_url", "artist_name"]
            target_table = "core.dim_artist"

        elif item_type == "podcasts":
            cleaning_func = self._clean_podcast
            columns = ["spotify_podcast_uri", "show_name", "description", "cover_art_url"]
            target_table = "core.dim_podcast"

        elif item_type == "episodes":
            cleaning_func = self._clean_episode
            columns = ["spotify_episode_uri", "duration_ms", "duration_sec", "show_name", "spotify_show_uri", "release_date"]
            target_table = "core.dim_episode"

    
        # query the staging layer for raw data and IDs
        staged_items = self.db.execute_query(f"SELECT record_id, raw_data FROM staging.spotify_{item_type}_data WHERE is_processed = FALSE;")
        if not staged_items:
            self.logger.warning("No unprocessed staged items found")
            return

        for i in range(0, len(staged_items), self.BATCH_SIZE):
            batch_number = i // self.BATCH_SIZE + 1
            # get the batch
            batch = staged_items[i:i+self.BATCH_SIZE]
            
            # batch ids to later delete
            batch_ids = []
            # clean rows to insert
            clean_rows = []

            for record_id, raw_data in batch:
                clean_data = cleaning_func(raw_data)
                # if there was an error while cleaning the json, skip over that record
                if not clean_data:
                    continue

                batch_ids.append(record_id)
                clean_rows.append(clean_data)

            # insert the batch inside a transaction
            with self.db.transaction() as tx_cursor:
                try:
                    query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
                    execute_values(tx_cursor, query, clean_rows)
                    inserted = tx_cursor.rowcount

                    # mark processed rows
                    tx_cursor.execute(f"UPDATE staging.spotify_{item_type}_data SET is_processed = TRUE WHERE record_id IN %s;", (tuple(batch_ids),))

                    self.logger.info(f"Batch {batch_number} done. Inserted {inserted} rows into {target_table}")
                    total_items_count += inserted
                
                except Exception as e:
                    self.logger.error(f"Error while inserting and updating batch number {batch_number}: {e}")
                    raise # raise so the transaction rolls back
        
        total_time = time.perf_counter() - time_start
        self.logger.info(f"All batches processed successfully in {total_time} seconds. Inserted {total_items_count} {item_type}")
        return total_time

    def run(self):
        
        item_types = ["tracks", "artists", "podcasts", "episodes"]
        total_time = 0.0

        for item_type in item_types:
            process_time = self.process_staged_batches(item_type)
            total_time += process_time

        self.logger.info(f"All items processed in {total_time} seconds")