import pytest
import time
from spotipy import SpotifyException

def test_log_error_batch(extractor, fake_logger, fake_db):

    batch = ["uri_1", "uri_2"]
    item_type = "track"

    extractor._log_error_batch(batch, item_type)

    fake_logger.warning.assert_called_with("Inserting failed batch into etl_internal.failed_uris")

    expected_error_batch = [(uri, item_type, "Failed batch") for uri in batch]

    fake_db.bulk_insert.assert_called_with(
        "etl_internal.failed_uris",
        ["uri", "entity_type", "error_reason"],
        expected_error_batch
    )


@pytest.mark.parametrize("entity_type, staged_history, core_items, staged_items, expected_new_items",[
    # for tracks, some items cored/staged already
    ("track",   ["uri1", "uri2", "uri3"], ["uri1"], ["uri2"], ["uri3"]),
    # for episodes, all items are new
    ("episode",   ["uri1", "uri2", "uri3"], [], [], ["uri1", "uri2", "uri3"]),
    # for artists, all items staged/cored already
    ("artist",  ["uri1", "uri2", "uri3"], ["uri1", "uri2"], ["uri3"], []),
    # for podcasts, all items cored
    ("podcast",  ["uri1", "uri2", "uri3"], ["uri1", "uri2", "uri3"], [], [])
])
def test_get_new_items(extractor, fake_db, entity_type, staged_history, core_items, staged_items, expected_new_items):
    if entity_type in ["artist", "podcast"]:
        fake_db.get_staged_uri_from_json.return_value = staged_history
    
    def fake_get_distinct_uri(uri_type, table):
        if table == "staging.streaming_history":
            return staged_history
        elif table == f"core.dim_{entity_type}":
            return core_items
        elif table == f"staging.spotify_{entity_type}s_data":
            return staged_items
        return []
        
    fake_db.get_distinct_uri.side_effect = fake_get_distinct_uri

    new_items = extractor._get_new_items(entity_type)

    assert sorted(new_items) == sorted(expected_new_items), f"For entity_type '{entity_type}', expected {expected_new_items} but got {new_items}"
    

@pytest.mark.parametrize("batch, expected_valid_count, expected_invalid_count",[
    (["valid_uri", "valid_uri"], 2, 0), # all items are valid
    (["valid_uri", "invalid_uri"], 1, 1), # half is valid
    (["invalid_uri", "invalid_uri"], 0, 2) # both invalid
])
def test_retry_batch(extractor, fake_db, batch, expected_valid_count, expected_invalid_count):
    def fake_api_call(item):
        if item == "valid_uri":
            return {"fake_data":"fake_data"}
        else:
            raise SpotifyException(http_status=400, msg="Invalid URI", code=None)
        
    valid_count, invalid_count = extractor._retry_batch(batch, "fake_item_type", fake_api_call)

    # check the result
    assert valid_count == expected_valid_count, f"Expected {expected_valid_count} valid items, got {valid_count}"
    assert invalid_count == expected_invalid_count, f"Expected {expected_invalid_count} invalid items, got {invalid_count}"

    # check the db calls
    if expected_invalid_count > 0:
        fake_db.bulk_insert.assert_any_call("etl_internal.failed_uris", 
                                               ["uri", "entity_type", "error_reason"], 
                                               [("invalid_uri", "fake_item_type", "Invalid URI")] * expected_invalid_count
                                               )
    
    fake_db.bulk_insert.assert_any_call("staging.spotify_fake_item_types_data", 
                                           ["spotify_fake_item_type_uri", "raw_data"], 
                                           [("valid_uri", {"fake_data": "fake_data"})] * expected_valid_count, 
                                           wrap_json=True
                                           )
    

def test_process_spotify_batch_success(fake_db, fake_logger, extractor):
    # fake api call to return a valid batch
    def fake_api_call(batch):
        return {"tracks": [{"uri": uri, "data": "valid_data"} for uri in batch]}
    
    batch = ["uri1","uri2", "uri3", "uri4"]

    success, _, items_count, failed_count = extractor._process_spotify_batch(batch, batch_number=1, api_call=fake_api_call, item_type="track")

    assert success is True
    assert items_count == len(batch)
    assert failed_count == 0
    fake_db.bulk_insert.assert_called_once()

    info_calls = [call.args[0] for call in fake_logger.info.call_args_list]
    assert any("Processed batch" in msg for msg in info_calls), "Expected a success log message."


def test_process_spotify_batch_rate_limit(fake_db, fake_logger, extractor, monkeypatch):
    call_counter = 0

    def fake_api_call(batch):
        nonlocal call_counter
        call_counter +=1

        if call_counter == 1:
            # raise a rate limit error on the first call
            e = SpotifyException(msg="Rate limit exceeded", http_status=429, headers={"Retry-After": "60"}, code=None)
            raise e
        else:
            # on the next try return valid data
            return {"tracks": [{"uri": uri, "data": "valid_data"} for uri in batch]}
        
    monkeypatch.setattr(time, "sleep", lambda x: None) # remove sleep

    batch = ["uri1","uri2", "uri3", "uri4"]

    success, _, items_count, failed_count = extractor._process_spotify_batch(batch, batch_number=1, api_call=fake_api_call, item_type="track")

    assert success is True
    assert items_count == len(batch)
    assert failed_count == 0

    fake_db.bulk_insert.assert_called_once()

    warning_calls = [call.args[0] for call in fake_logger.warning.call_args_list]
    assert any("exceeded rate limit" in msg for msg in warning_calls), "Expected a rate limit warning."


def test_process_spotify_batch_invalid_uri(fake_db, fake_logger, extractor, monkeypatch):
    def fake_api_call(batch):
        raise SpotifyException(msg="Invalid URI", http_status=400, code=None)
    
    def fake_retry_batch(batch, item_type, api_call):
        # return test items_count, failed_items_counter
        return (1, 3)
    
    extractor._retry_batch = fake_retry_batch

    batch = ["uri1","uri2", "uri3", "uri4"]

    success, _, items_count, failed_count = extractor._process_spotify_batch(batch, batch_number=1, api_call=fake_api_call, item_type="track")

    assert success is True
    assert items_count == 1
    assert failed_count == 3

    error_calls = [call.args[0] for call in fake_logger.error.call_args_list]
    assert any("HTTP 400" in msg for msg in error_calls), "Expected an HTTP 400 error log."


def test_process_spotify_batch_exhaust_retries(fake_db, fake_logger, extractor, monkeypatch, mocker):
    def fake_api_call(batch):
        raise SpotifyException(msg="Rate limit exceeded", http_status=429, headers={"Retry-After": "60"}, code=None)
    
    monkeypatch.setattr(time, "sleep", lambda x: None) # remove sleep

    extractor._log_error_batch = mocker.MagicMock()

    batch = ["uri1", "uri2", "uri3", "uri4"]

    success, _, items_count, failed_count = extractor._process_spotify_batch(batch, batch_number=1, api_call=fake_api_call, item_type="track")

    assert success is False
    assert items_count == 0
    assert failed_count == 4
    
    extractor._log_error_batch.assert_called_once()

    error_calls = [call.args[0] for call in fake_logger.error.call_args_list]
    assert any("Exceeded retries" in msg for msg in error_calls), "Expected an 'Exceeded retries' error log."