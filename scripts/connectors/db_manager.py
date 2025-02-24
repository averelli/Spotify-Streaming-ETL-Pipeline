import psycopg2
from psycopg2.extras import execute_values
from config.config import settings
import logging

class DatabaseManager:
    def __init__(self, logger):
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

    def bulk_insert(self, table_name, columns, records):
        """Insert multiple rows using execute_values."""
        if not records:
            self.logger.warning("No records to insert.")
            return

        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s ON CONFLICT DO NOTHING"
        try:
            execute_values(self.cursor, query, records)
            self.connection.commit()
        except Exception as e:
            self.logger.error(f"Error in bulk insert: {e}")
            self.connection.rollback()

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
            uri_type (str): Either 'track' or 'episode'
            table (str): The table from which to fetch the URIs
        
        Returns:
            list: A list of distinct URIs
        """
        if uri_type not in ["track", "episode"]:
            raise ValueError("Wrong value for uri_type, only 'track' or 'episode' are valid.")
        
        query = f"SELECT DISTINCT spotify_{uri_type}_uri FROM {table}"
        result = self.execute_query(query)
        self.logger.info(f"Fetched {len(result)} distinct {uri_type} URIs from {table}")
        return [row[0] for row in result]
    