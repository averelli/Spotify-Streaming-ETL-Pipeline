from scripts.connectors.db_manager import DatabaseManager
from scripts.connectors.spotify_client import SpotifyClient 
import json
import glob
import os
import logging
import time
from datetime import datetime, timezone
from spotipy.exceptions import SpotifyException

class DataExtractor:
    def __init__(self, db: DatabaseManager, logger: logging.Logger):
        self.db = db
        self.logger = logger
        self.spotify_client = SpotifyClient(logger)

    def extract_streaming_history(self):
        # metrics
        total_files = 0
        total_records = 0
        total_time = 0.0

        # get the latest timestamp 
        max_ts = self.db.execute_query("SELECT MAX(ts) FROM core.fact_streaming_history")
        if max_ts == None:
            max_ts = datetime(1900, 1, 1, tzinfo=timezone.utc)
        
        # iterate over raw files
        raw_data_path = os.path.join(os.getcwd(), "data/raw")
        for json_file in glob.glob(os.path.join(raw_data_path, "*.json")):
            filename = os.path.basename(json_file)
            file_start_time = time.time()
            self.logger.info(f"Started processing for file: {filename}")
            
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                    columns = ["ts", "platform", "ms_played", "conn_country", "ip_addr", "master_metadata_track_name", "master_metadata_album_artist_name", "master_metadata_album_album_name", "spotify_track_uri", "episode_name", "episode_show_name", "spotify_episode_uri", "reason_start", "reason_end", "shuffle", "skipped", "offline", "offline_timestamp", "incognito_mode"]

                    # create records to insert only if the timestamp is later than the max recorded one
                    records = [
                        (
                            row["ts"],
                            row["platform"],
                            row["ms_played"],
                            row["conn_country"],
                            row["ip_addr"],
                            row["master_metadata_track_name"],
                            row["master_metadata_album_artist_name"],
                            row["master_metadata_album_album_name"],
                            row["spotify_track_uri"],
                            row["episode_name"],
                            row["episode_show_name"],
                            row["spotify_episode_uri"],
                            row["reason_start"],
                            row["reason_end"],
                            row["shuffle"],
                            row["skipped"],
                            row["offline"],
                            row["offline_timestamp"],
                            row["incognito_mode"]
                        ) for row in data if datetime.strptime(row["ts"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) > max_ts
                    ]

                    # empty file check
                    if len(records) == 0:
                        self.logger.warning(f"Empty file detected: {filename}")
                    else:   
                        self.db.bulk_insert("staging.streaming_history", columns, records)
            
                # Log success
                processing_time = time.time() - file_start_time
                record_count = len(records)
                self.logger.info(f"Successfully processed {filename}: {record_count} records in {processing_time:.2f} seconds")
            
                total_files += 1
                total_records += record_count
                total_time += processing_time

            except json.JSONDecodeError as e:
                self.logger.error(f"JSON error in {json_file}: {e}")
            except IOError as e:
                self.logger.error(f"Could not read {json_file}: {e}")
            except Exception as e:
                self.logger.error(f"Unexpected error processing {json_file}: {str(e)}", exc_info=True)

        # Final log
        if total_files == 0:
            self.logger.warning("No files processed during extraction")
        else:
            self.logger.info(f"Extraction complete. Processed {total_files} files, {total_records} total records. Total time: {total_time:.2f} seconds")


    def fetch_spotify_entities(self):
        total_time = 0.0
        total_tracks_processed = 0

        # get new unique tracks to process
        new_tracks = self._get_new_tracks()

        # num of tracks in one batch (max = 50)
        batch_size = 50
        batch_iterator = 0

        # main batch processing loop
        while batch_iterator < len(new_tracks):
            batch_number = batch_iterator // 50 + 1

            self.logger.info(f"Started processing batch number: {batch_number}")
            
            batch = new_tracks[batch_iterator:batch_iterator+batch_size]

            success, batch_time, tracks_count = self._process_spotify_batch(batch=batch, 
                                                                            batch_number=batch_number,
                                                                            api_call=self.spotify_client.get_tracks,
                                                                            target_table="spotify_tracks_data")

            total_time += batch_time
            total_tracks_processed += tracks_count
            
            if not success:
                self.logger.error(f"Batch {batch_number} failed after maximum retries. Skipping these URIs.")
                
            batch_iterator += batch_size

        self.logger.info(f"All batches processed. Total time: {total_time:.2f} seconds. Total tracks: {total_tracks_processed}")


    def _process_spotify_batch(self, batch: list, batch_number:int, api_call:function, target_table:str) -> tuple[bool, float, int]:
        """
        Process a single batch of tracks.
        Params:
            batch (list): a list of Spotify URIs to process
            batch_number (int): batch number for logging
            api_call (function): Spotify client function to fetch data (e.g. get_tracks())
            target_table (str): target table to insert data insert the raw data, must be inside the staging layer
        Returns:
            (bool, float, int): (success flag, batch processing time, number of tracks processed)
        """
        retry_counter = 0
        batch_time_start = time.now()  
        
        while retry_counter < 2:
            try:
                # call Spotify API to get data
                api_response = api_call(batch)
                # from API we get a dict like: {'tracks': [tracks data]} so to insert into staging each track as individual row we select the list 
                data_key = list(api_response.keys())[0]
                data_items = api_response[data_key]
                items_count = len(data_items)

                # insert raw data into staging
                self.db.bulk_insert(f"staging.{target_table}", ["raw_data"], data_items)
                
                # track and log the time
                batch_total_time = time.now() - batch_time_start
                self.logger.info(f"Processed batch {batch_number} with {items_count} in {batch_total_time:.2f} seconds on attempt {retry_counter+1}")

                return True, batch_total_time, items_count
            
            except SpotifyException as e:
                # catch a Spotify error: either a rate limit or something wrong with credentials 

                batch_total_time = time.now() - batch_time_start

                if e.http_status == 429:  # rate limit error
                    wait_time = int(e.headers.get("Retry-After", 60))
                    self.logger.warning(f"Batch {batch_number} exceeded rate limit. Attempt {retry_counter+1} took {batch_total_time:.2f} seconds. Waiting for {wait_time} seconds.")
                    time.sleep(wait_time)
                    
                    retry_counter += 1

                else:
                    self.logger.error(f"Spotify error in batch {batch_number}: {e}")
                    raise
            
            except Exception as e:
                batch_total_time = time.now() - batch_time_start
                self.logger.error(f"Unexpected error in batch {batch_number} after {batch_total_time:.2f} seconds: {e}")
                raise
        
        # if retries fail return False
        self.logger.error(f"Exceeded retries for batch {batch_number}")
        return False, batch_total_time, 0

    def _get_new_tracks(self):
        """Returns a list of only the new unique tracks from the staged data"""
        staged_tracks = self.db.get_distinct_uri(uri_type = "track", table="staging.streaming_history")
        existing_tracks = self.db.get_distinct_uri(uri_type="tracks", table="core.dim_track")

        new_tracks = list(set(staged_tracks) - set(existing_tracks))

        return new_tracks

    