import pytest

@pytest.fixture
def fake_db(mocker):
    return mocker.MagicMock()
    
@pytest.fixture
def fake_logger(mocker):
    return mocker.MagicMock()
