from scripts.connectors.db_manager import DatabaseManager 
from psycopg2.extras import execute_values
import logging

class DataTransformer:
    def __init__(self, db: DatabaseManager, logger: logging.Logger):
        self.db = db
        self.logger = logger
        
        self.BATCH_SIZE = 50

    def _process_track(self, raw_track:dict):
        """
        Takes in a single track dict and returns a clean dict with 
        Args:
            raw_track (dict): raw json from staging
        Returns:
            tuple: clean track row
        """
        # normalise the release date
        if raw_track["album"]["release_date_precision"] == "year":
            clean_date = raw_track["album"]["release_date"] + "-01-01"
        elif raw_track["album"]["release_date_precision"] == "month":
            clean_date = raw_track["album"]["release_date"] + "-01"
        else:
            clean_date =raw_track["album"]["release_date"]

        clean_track = (
            raw_track["uri"], 
            raw_track["name"], 
            raw_track["album"]["images"][0]["url"], 
            raw_track["album"]["name"], 
            raw_track["album"]["id"], 
            raw_track["album"]["album_type"], 
            raw_track["artists"][0]["name"], 
            clean_date, 
            raw_track["duration_ms"], 
            int(round(raw_track["duration_ms"] / 1000, 0))
        )
        
        return clean_track
        
    def process_staged_batches(self, item_type:str):
        # based on the item type, select the cleaning function and columns list
        cleaning_func = None
        columns = []
        target_table = None
        if item_type == "tracks":
            cleaning_func = self._process_track
            columns = ["spotify_track_uri", "track_title", "cover_art_url", "album_name", "album_spotify_id", "album_type", "artist_name", "release_date", "duration_ms", "duration_sec"]
            target_table = "core.dim_track"

    
        # query the staging layer for raw data and IDs
        staged_items = self.db.execute_query(f"SELECT record_id, raw_data FROM staging.spotify_{item_type}_data WHERE is_processed = FALSE limit 80;")

        for i in range(0, len(staged_items), self.BATCH_SIZE):
            batch_number = i // self.BATCH_SIZE + 1
            # get the batch
            batch = staged_items[i:i+self.BATCH_SIZE]
            
            # batch ids to later delete
            batch_ids = []
            # clean rows to insert
            clean_rows = []

            for record_id, raw_data in batch:
                batch_ids.append(record_id)
                clean_data = cleaning_func(raw_data)
                clean_rows.append(clean_data)

            # insert the batch inside a transaction
            with self.db.transaction() as tx_cursor:
                query = f"INSERT INTO {target_table} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
                execute_values(tx_cursor, query, clean_rows)

                # mark processed rows
                tx_cursor.execute(f"UPDATE staging.spotify_{item_type}_data SET is_processed = TRUE WHERE record_id IN %s;", (tuple(batch_ids),))

            
            


