import psycopg2
from psycopg2.extras import execute_values, Json
from config.config import settings
from logging import Logger
from datetime import datetime, timezone

class DatabaseManager:
    def __init__(self, logger:Logger):
        self.connection = None
        self.cursor = None
        self.logger = logger
        self.connect()

    def connect(self):
        """Establish a database connection using DATABASE_URL."""
        try:
            self.connection = psycopg2.connect(settings.DATABASE_URL)
            self.cursor = self.connection.cursor()
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e.__str__()}")

    def execute_query(self, query, params=None):
        """Execute a single query."""
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            self.connection.commit()

            if query.strip().upper().startswith("SELECT"):
                return self.cursor.fetchall()
            
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            self.connection.rollback()
            return None

    def bulk_insert(self, table_name, columns, records, wrap_json:bool = False):
        """Insert multiple rows using execute_values.
    
        Args:
            table_name (str): Target table name.
            columns (list): List of columns to insert into.
            records (list): List of record tuples.
            wrap_json (bool): If True, wrap dictionary values with psycopg2.extras.Json.
        """
        if not records:
            self.logger.warning("No records to insert.")
            return
        
        if wrap_json:
            # also include the item's uri as id
            records = [(item.get("uri"), Json(item),) for item in records]

        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        try:
            execute_values(self.cursor, query, records)
            self.connection.commit()
        except Exception as e:
            self.logger.error(f"Error in bulk insert: {e}")
            self.connection.rollback()
            raise

    def close(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def get_distinct_uri(self, uri_type:str, table:str):
        """
        Fetches distinct URIs from the staged data

        Params:
            uri_type (str): Either 'track', 'artist', 'podcast' or 'episode'
            table (str): The table from which to fetch the URIs
        
        Returns:
            list: A list of distinct URIs
        """
        if uri_type not in ["track", "episode", "artist", "podcast"]:
            raise ValueError("Invalid uri_type. Must be: track, episode, artist, or podcast")
        
        query = f"SELECT DISTINCT spotify_{uri_type}_uri FROM {table};"
        result = self.execute_query(query)
        self.logger.info(f"Fetched {len(result)} distinct {uri_type} URIs from {table}")
        
        return [row[0] for row in result if row[0] is not None]
    
    def get_staged_uri_from_json(self, uri_type:str):
        
        if uri_type == "artist":
            query = "SELECT DISTINCT artists ->> 'uri' FROM staging.spotify_tracks_data t, jsonb_array_elements(raw_data -> 'album' -> 'artists') as artists;"
        elif uri_type == "podcast":
            query = "select distinct raw_data -> 'show' ->> 'uri' from staging.spotify_episodes_data t;"

        else:
            raise ValueError("Wrong value for uri_type, only 'artist' or 'podcast' are valid.")
        
        result = self.execute_query(query)
        self.logger.info(f"Fetched {len(result)} distinct {uri_type} URIs from staged items")

        return [row[0] for row in result]
    
    
    def get_max_history_ts(self):
        """Returns the latest date from the core and staged streaming history"""
        max_ts = self.execute_query(
            """
            SELECT GREATEST(
                (SELECT MAX(ts_msk AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC') FROM core.fact_tracks_history),
                (SELECT MAX(ts_msk AT TIME ZONE 'Europe/Moscow' AT TIME ZONE 'UTC') FROM core.fact_podcasts_history),
                (SELECT MAX(ts) FROM staging.streaming_history)
            ) AS max_msk_timestamp;
            """
        )[0][0]
        if max_ts == None:
            max_ts = datetime(1900, 1, 1, tzinfo=timezone.utc)

        return max_ts