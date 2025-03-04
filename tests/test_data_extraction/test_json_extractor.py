import os
import json
import pytest
from datetime import datetime, timezone

#Create a temp directory structure and a test json
@pytest.fixture
def create_test_file(tmp_path):
    data_raw = tmp_path / "data" / "raw"
    data_raw.mkdir(parents=True, exist_ok=True)

    test_file = data_raw / "test_data.json"
    test_data = [
        {
            "ts": "2021-01-01T00:00:00Z",
            "platform": "web",
            "ms_played": 3000,
            "conn_country": "US",
            "ip_addr": "127.0.0.1",
            "master_metadata_track_name": "Test Track",
            "master_metadata_album_artist_name": "Test Artist",
            "master_metadata_album_album_name": "Test Album",
            "spotify_track_uri": "spotify:track:123",
            "episode_name": "",
            "episode_show_name": "",
            "spotify_episode_uri": "",
            "reason_start": "test",
            "reason_end": "test",
            "shuffle": False,
            "skipped": False,
            "offline": False,
            "offline_timestamp": None,
            "incognito_mode": False
        },
        {
            "ts": "2023-01-01T00:00:00Z",
            "platform": "web",
            "ms_played": 3000,
            "conn_country": "US",
            "ip_addr": "127.0.0.1",
            "master_metadata_track_name": "Test Track",
            "master_metadata_album_artist_name": "Test Artist",
            "master_metadata_album_album_name": "Test Album",
            "spotify_track_uri": "spotify:track:123",
            "episode_name": "",
            "episode_show_name": "",
            "spotify_episode_uri": "",
            "reason_start": "test",
            "reason_end": "test",
            "shuffle": False,
            "skipped": False,
            "offline": False,
            "offline_timestamp": None,
            "incognito_mode": False
        }
    ]

    with open(test_file, "w", encoding="utf-8") as f:
        json.dump(test_data, f)
    
    return tmp_path


@pytest.mark.parametrize("get_max_history_ts, expected_count", [
    (datetime(2020, 1, 1, tzinfo=timezone.utc), 2),  # Both records should be inserted
    (datetime(2022, 1, 1, tzinfo=timezone.utc), 1),  # Only the second record should be inserted
    (datetime(2023, 1, 1, tzinfo=timezone.utc), 0),  # No records should be inserted
])
def test_extract_streaming_history_success(extractor, fake_db, fake_logger, create_test_file, get_max_history_ts, expected_count, monkeypatch):
    monkeypatch.setattr(os, "getcwd", lambda: str(create_test_file))

    # Override fake_db.get_max_history_ts for this test run
    fake_db.get_max_history_ts.return_value = get_max_history_ts

    extractor.extract_streaming_history()

    fake_db.get_max_history_ts.assert_called_once()

    if expected_count > 0:
        fake_db.bulk_insert.assert_called_once()
        args, _ = fake_db.bulk_insert.call_args
        records = args[2]  # Extract the inserted records
        assert len(records) == expected_count, f"Expected {expected_count} record(s), got {len(records)}"
    else:
        fake_db.bulk_insert.assert_not_called()
    
    info_calls = [call.args[0] for call in fake_logger.info.call_args_list]
    assert any("Extraction complete" in message for message in info_calls), "Expected a success log message"


def test_extract_streaming_history_json_error(extractor, fake_db, fake_logger, tmp_path, monkeypatch):
    data_raw = tmp_path / "data" / "raw"
    data_raw.mkdir(parents=True, exist_ok=True)
    bad_file = data_raw / "bad.json"
    # Write invalid JSON content.
    bad_file.write_text("this is not valid json", encoding="utf-8")
    
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    
    extractor.extract_streaming_history()
    
    # Check that the logger captured a JSON error.
    error_messages = [call.args[0] for call in fake_logger.error.call_args_list]
    assert any("JSON error" in msg for msg in error_messages), "Expected a JSON error log message."


def test_extract_streaming_history_io_error(extractor, fake_db, fake_logger, tmp_path, monkeypatch):
    data_raw = tmp_path / "data" / "raw"
    data_raw.mkdir(parents=True, exist_ok=True)
    test_file = data_raw / "test_data.json"
    test_file.write_text("{}", encoding="utf-8")
    
    # Monkeypatch open to raise an IOError when attempting to read any file.
    def broken_open(*args, **kwargs):
        raise IOError("Test IOError")
    monkeypatch.setattr("builtins.open", broken_open)
    
    monkeypatch.setattr(os, "getcwd", lambda: str(tmp_path))
    
    extractor.extract_streaming_history()
    
    # Check that the logger captured an IOError message.
    error_messages = [call.args[0] for call in fake_logger.error.call_args_list]
    assert any("Could not read" in msg for msg in error_messages), "Expected an IOError log message."
