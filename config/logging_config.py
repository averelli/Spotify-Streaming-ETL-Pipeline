import logging
import logging.handlers
import json
import os

def setup_logging():
    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(logging.DEBUG)  

    # Create a formatter
    formatter = logging.Formatter(json.dumps({
        "timestamp": "%(asctime)s",
        "level": "%(levelname)s",
        "message": "%(message)s",
        "module": "%(module)s",
        "function": "%(funcName)s",
        "line": "%(lineno)d"
    }))

    # Log to console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Ensure 'logs/' directory exists
    os.makedirs("logs", exist_ok=True)

    # Log to file with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        "logs/etl_pipeline.log",
        maxBytes=10 * 1024 * 1024,  # 10 MB per file
        backupCount=5  
    )
    file_handler.setLevel(logging.DEBUG)  
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger