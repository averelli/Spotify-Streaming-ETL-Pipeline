from scripts.etl.extractor import DataExtractor
from scripts.etl.transformer import DataTransformer
from scripts.connectors.db_manager import DatabaseManager
from logging import Logger

class ETL():
    def __init__(self, db: DatabaseManager, logger: Logger):
        self.db = db
        self.logger = logger
        self.extractor = DataExtractor(db, logger)
        self.transformer = DataTransformer(db, logger)

    def run(self):
        self.logger.info("Starting ETL process")

        try:
            extraction_time = self.extractor.run()
            transformation_time = self.transformer.run()
            total_time = extraction_time + transformation_time

            self.logger.info(f"ETL process finished. Extraction took {extraction_time} seconds, transformation took {transformation_time} seconds, total time {total_time} seconds")
        
        except Exception as e:
            self.logger.error(f"ETL process failed: {e}", exc_info=True)
            raise