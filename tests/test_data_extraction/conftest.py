import pytest
from scripts.etl.extractor import DataExtractor

@pytest.fixture
def extractor(fake_db, fake_logger):
    return DataExtractor(fake_db, fake_logger)