from scripts.etl.etl import ETL
from config.logging_config import setup_logging
from scripts.connectors.db_manager import DatabaseManager

def main():
    logger = setup_logging()

    with DatabaseManager(logger) as db:
        etl = ETL(db, logger)
        etl.run()

if __name__ == "__main__":
    main()