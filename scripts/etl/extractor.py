from scripts.connectors.db_manager import DatabaseManager
from scripts.connectors.spotify_client import SpotifyClient 
import json
import glob
import os
import logging
import time
from datetime import datetime, timezone
from spotipy.exceptions import SpotifyException
from typing import Callable

class DataExtractor:
    def __init__(self, db: DatabaseManager, logger: logging.Logger):
        self.db = db
        self.logger = logger
        self.spotify_client = SpotifyClient(logger)

    def extract_streaming_history(self):
        """
        Extracts streaming data from raw json files provided by Spotify and inserts them into the staging layer of db.
        """
        # metrics
        total_files = 0
        total_records = 0
        total_time = 0.0

        # get the latest timestamp 
        max_ts = self.db.get_max_history_ts()
        
        # iterate over raw files
        raw_data_path = os.path.join(os.getcwd(), "data/raw")
        for json_file in glob.glob(os.path.join(raw_data_path, "*.json")):
            filename = os.path.basename(json_file)
            file_start_time = time.perf_counter()
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
                        self.logger.info(f"Empty file or nothing to insert: {filename}")
                    else:
                        self.db.bulk_insert("staging.streaming_history", columns, records)
            
                # Log success
                processing_time = time.perf_counter() - file_start_time
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

    def stage_spotify_items(self, item_type:str):
        """
        Stage unique Spotify entities from streaming history

        Args:
            item_type (str): `track`, `episode`, `artist` or `podcast`
        """
        total_time = 0.0
        total_items_processed = 0
        total_failed_items = 0

        # get new unique tracks or episodes to process
        new_items = self._get_new_items(item_type)

        # num of items in one batch (max = 50)
        batch_size = 50

        # main batch processing loop
        for i in range(0, len(new_items), batch_size):
            batch_number = i // batch_size + 1
            batch = new_items[i:i+batch_size]

            self.logger.info(f"Started processing batch number: {batch_number}")

            # API call function for each type
            api_calls = {
                "track": self.spotify_client.get_tracks,
                "episode": self.spotify_client.get_episodes,
                "artist": self.spotify_client.get_artists,
                "podcast": self.spotify_client.get_podcasts
            }

            success, batch_time, items_count, failed_items = self._process_spotify_batch(batch=batch, 
                                                                            batch_number=batch_number,
                                                                            api_call=api_calls[item_type],
                                                                            item_type = item_type
                                                                            )

            total_time += batch_time
            total_items_processed += items_count
            total_failed_items += failed_items
            
            if not success:
                self.logger.error(f"Batch {batch_number} failed after maximum retries. Skipping these URIs.")

        self.logger.info(f"All batches processed. Total time: {total_time:.2f} seconds. Total {item_type}s: {total_items_processed} with {total_failed_items} {item_type}s failed")

    def _process_spotify_batch(self, batch: list, batch_number:int, api_call:Callable, item_type:str, retry_limit:int = 2) -> tuple[bool, float, int, int]:
        """
        Process a single batch of tracks or episodes.
        Args:
            batch (list): a list of Spotify URIs to process
            batch_number (int): batch number for logging
            api_call (Callable): Spotify client function to fetch data (e.g. get_tracks())
            item_type (str): `track`, `episode`, `artist` or `podcast`
            retry_limit (int): Number of retries allowed before failing. 2 by default
        Returns:
            tuple[bool, float, int, int]: (success flag, batch processing time, number of items processed, number of failed items)

        """
        retry_counter = 0
        failed_items_counter = 0
        batch_time_start = time.perf_counter()  
        
        while retry_counter < retry_limit:
            try:
                # call Spotify API to get data
                api_response = api_call(batch)
                # from API we get a dict like: {'tracks': [tracks data]} so to insert into staging each track as individual row we select the list 
                data_key = list(api_response.keys())[0]
                data_items = api_response[data_key]
                items_count = len(data_items)

                # Extract IDs from the response to map URIs
                fetched_data = {item.get("uri"): item for item in data_items if item}  # Filter out None values

                # Identify URIs that returned null and log them into db
                failed_uris = [(uri, item_type, "API returned null") for uri in batch if fetched_data.get(uri) is None]
                failed_items_counter = len(failed_uris)
                if failed_items_counter >= 1:
                    self.logger.warning(f"{failed_items_counter} failed URIs detected")
                    self.db.bulk_insert("etl_internal.failed_uris", ["uri", "entity_type", "error_reason"], failed_uris)

                # Convert dict to list for insertion
                valid_data = list(fetched_data.values())

                # insert raw data into staging
                self.db.bulk_insert(f"staging.spotify_{item_type}s_data", [f"spotify_{item_type}_uri", "raw_data"], valid_data, wrap_json=True)
                
                # track and log the time
                batch_total_time = time.perf_counter() - batch_time_start
                self.logger.info(f"Processed batch {batch_number} with {items_count} items in {batch_total_time:.2f} seconds on attempt {retry_counter+1}")

                return True, batch_total_time, items_count, failed_items_counter
            
            except SpotifyException as e:
                # catch a Spotify error: either a rate limit or something wrong with credentials 

                batch_total_time = time.perf_counter() - batch_time_start

                if e.http_status == 429:  # rate limit error
                    wait_time = int(e.headers.get("Retry-After", 60))
                    self.logger.warning(f"Batch {batch_number} exceeded rate limit. Attempt {retry_counter+1} took {batch_total_time:.2f} seconds. Waiting for {wait_time} seconds.")
                    time.sleep(wait_time)
                    
                    retry_counter += 1

                elif e.http_status == 400: # invalid uri
                    self.logger.error(f"Batch {batch_number} failed with HTTP 400. Retrying items individually.")
                    items_count, failed_items_counter = self._retry_batch(batch, item_type, api_call)

                    batch_total_time = time.perf_counter() - batch_time_start
                    self.logger.info(f"Processed batch {batch_number} with {items_count} successfully with {failed_items_counter} invalid URIs in {batch_total_time:.2f} seconds on attempt {retry_counter+1}")

                    return True, batch_total_time, items_count, failed_items_counter

                else:
                    self.logger.error(f"Spotify error in batch {batch_number}: {e}")
                    raise
            
            except Exception as e:
                batch_total_time = time.perf_counter() - batch_time_start
                self.logger.error(f"Unexpected error in batch {batch_number} after {batch_total_time:.2f} seconds: {e}")
                raise
        
        # if retries fail return False and log failed URIs
        self.logger.error(f"Exceeded retries for batch {batch_number}")
        self._log_error_batch(batch, item_type)

        return False, batch_total_time, 0, len(batch)

    def _get_new_items(self, entity_type: str):
        """
        Returns a list of only the new unique items from the staged data, 
        excluding those already in the core and previous staging history.

        Args:
            entity_type (str): `track` or `episode`
        Returns:
            list: new items to process
        """
        # URIs already staged in history
        if entity_type in ["artist", "podcast"]:
            staged_history_items = self.db.get_staged_uri_from_json(uri_type=entity_type)
        else:
            staged_history_items = self.db.get_distinct_uri(uri_type=entity_type, table="staging.streaming_history")

        # URIs already in the core dimension table
        existing_core_items = self.db.get_distinct_uri(uri_type=entity_type, table=f"core.dim_{entity_type}")

        # URIs from the current staging load
        staged_items = self.db.get_distinct_uri(uri_type=entity_type, table=f"staging.spotify_{entity_type}s_data")

        # Exclude already processed and previously staged URIs
        new_items = list(set(staged_history_items) - set(existing_core_items) - set(staged_items))

        return new_items

    def _log_error_batch(self, batch:list, item_type:str):
        """
        Logs the failed batch of items into the database.

        Args:
            batch (list): List of items that failed.
            item_type (str): Type of the item, can be `track`, `episode`, `artist` or `podcast`.
        """
        
        self.logger.warning("Inserting failed batch into etl_internal.failed_uris")
        error_batch = [(uri, item_type, "Failed batch") for uri in batch]
        self.db.bulk_insert("etl_internal.failed_uris", ["uri", "entity_type", "error_reason"], error_batch)
  
    def _retry_batch(self, batch:list, item_type:str, api_call:Callable):
        """
        Retries the failed batch of items.

        Args:
            batch (list): List of items to retry.
            item_type (str): Type of the item, can be `track`, `episode`, `artist` or `podcast`.
            api_call (Callable): API call function
        Returns:
            tuple[int, int]: A tuple containing the number of valid and invalid items.
        """
        invalid_uris = []
        valid_data = []

        for item in batch:
            try:
                item_data = api_call(item)
                # append item uri and the data
                valid_data.append((item, item_data))

            except SpotifyException as single_e:
                if single_e.http_status == 400:
                    self.logger.warning(f"Invalid URI detected: {item}")
                    invalid_uris.append((item, item_type, "Invalid URI"))
                else:
                    raise  # If another error occurs, let it bubble up
            
        if invalid_uris:
            self.logger.info(f"Logging {len(invalid_uris)} invalid URIs to etl_internal.failed_uris")
            self.db.bulk_insert("etl_internal.failed_uris", ["uri", "entity_type", "error_reason"], invalid_uris)

        # Insert valid URIs
        self.db.bulk_insert(f"staging.spotify_{item_type}s_data", [f"spotify_{item_type}_uri", "raw_data"], valid_data, wrap_json=True)

        return len(valid_data), len(invalid_uris)

    def run(self):
        """
        Orchestrates the full data extraction process. 
        Returns:
            int: Total time for the entire process.
        """

        start_time = time.perf_counter()
        self.logger.info("Started running data extraction.")

        # extract data from jsons
        self.extract_streaming_history()
        
        # fetch data from Spotify API
        item_types = ["track", "artist", "podcast", "episode"]
        for item_type in item_types:
            self.stage_spotify_items(item_type)

        total_time = round(time.perf_counter() - start_time, 2)
        self.logger.info(f"Data extraction finished, took {total_time} seconds")

        return total_time