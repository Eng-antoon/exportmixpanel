import os
import sys
import json
import pytest
from io import StringIO
import tempfile
import shutil
import openpyxl
from datetime import datetime

# Adjust sys.path to import modules from the project root
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import (app, normalize_carrier, load_excel_data, get_saved_filters, 
                 save_filter_to_session, fetch_api_token, fetch_api_token_alternative, 
                 determine_completed_by, migrate_db)


# ---------------- Test normalize_carrier ----------------

def test_normalize_carrier_none():
    assert normalize_carrier(None) == ""


def test_normalize_carrier_empty():
    assert normalize_carrier("") == ""


def test_normalize_carrier_vodafone():
    # Test various casing and spacing
    assert normalize_carrier("Voda fOne") == "Vodafone"


def test_normalize_carrier_orange():
    assert normalize_carrier("  orangeeg  ") == "Orange"


def test_normalize_carrier_unknown():
    # If not matched, should return title case
    assert normalize_carrier("randomcarrier") == "Randomcarrier"


# ---------------- Test load_excel_data ----------------

def create_dummy_excel(file_path, header, rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(header)
    for row in rows:
        ws.append(row)
    wb.save(file_path)


def test_load_excel_data_success(tmp_path):
    file_path = tmp_path / "dummy.xlsx"
    header = ["col1", "col2"]
    rows = [[1, 2], [3, 4]]
    create_dummy_excel(file_path, header, rows)
    data = load_excel_data(str(file_path))
    assert len(data) == 2
    assert data[0]["col1"] == 1


def test_load_excel_data_empty(tmp_path):
    file_path = tmp_path / "empty.xlsx"
    header = ["col1", "col2"]
    rows = []
    create_dummy_excel(file_path, header, rows)
    data = load_excel_data(str(file_path))
    assert data == []


def test_load_excel_data_no_header(tmp_path):
    # Create an excel file with header row only
    file_path = tmp_path / "noheader.xlsx"
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["col1", "col2"])
    wb.save(file_path)
    data = load_excel_data(str(file_path))
    # With only headers and no data rows, expect empty data list
    assert data == []


# ---------------- Test session utility functions ----------------

def test_session_filters():
    with app.test_request_context('/'):
        from flask import session as flask_session
        # Initially, saved filters should be empty
        assert get_saved_filters() == {}
        save_filter_to_session("test", {"filter": "value"})
        saved = get_saved_filters()
        assert "test" in saved
        assert saved["test"]["filter"] == "value"


# ---------------- Test fetch_api_token and alternative ----------------

def fake_post_success(*args, **kwargs):
    class FakeResponse:
        def __init__(self, json_data, status_code):
            self._json = json_data
            self.status_code = status_code
        def json(self):
            return self._json
        def raise_for_status(self):
            if not (200 <= self.status_code < 300):
                raise Exception('HTTP Error')
    return FakeResponse({"token": "abc123"}, 200)


def fake_post_failure(*args, **kwargs):
    class FakeResponse:
        def __init__(self, text, status_code):
            self.text = text
            self.status_code = status_code
        def json(self):
            return {}
    return FakeResponse("Error", 400)


def test_fetch_api_token_success(monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", fake_post_success)
    token = fetch_api_token()
    assert token == "abc123"


def test_fetch_api_token_failure(monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", fake_post_failure)
    token = fetch_api_token()
    assert token is None


def test_fetch_api_token_alternative_success(monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", fake_post_success)
    token = fetch_api_token_alternative()
    assert token == "abc123"


def test_fetch_api_token_alternative_failure(monkeypatch):
    import requests
    monkeypatch.setattr(requests, "post", fake_post_failure)
    token = fetch_api_token_alternative()
    assert token is None


# ---------------- Test determine_completed_by ----------------
# Note: This function is expected to return the user_type of the latest event with a valid 'completed' status.
# If no valid event is found, it should return None.


def test_determine_completed_by_no_events():
    result = determine_completed_by([])
    assert result is None


def test_determine_completed_by_invalid_event():
    # Event missing 'changes' or proper structure
    event = {"created_at": "2023-01-01 10:00:00"}
    result = determine_completed_by([event])
    assert result is None


def test_determine_completed_by_valid_event():
    # Single valid event
    event = {
        "changes": {"status": ["pending", "completed"]},
        "created_at": "2023-01-01 10:00:00",
        "user_type": "admin"
    }
    result = determine_completed_by([event])
    # Assuming the correct implementation returns the user_type
    assert result == "admin"


def test_determine_completed_by_multiple_events():
    # Two events, the event with later time should be returned
    event1 = {
        "changes": {"status": ["pending", "completed"]},
        "created_at": "2023-01-01 10:00:00",
        "user_type": "admin"
    }
    event2 = {
        "changes": {"status": ["pending", "completed"]},
        "created_at": "2023-01-01 12:00:00",
        "user_type": "driver"
    }
    result = determine_completed_by([event1, event2])
    assert result == "driver"


def test_determine_completed_by_invalid_date():
    # Event with an invalid date format should be skipped
    event = {
        "changes": {"status": ["pending", "completed"]},
        "created_at": "invalid_date",
        "user_type": "driver"
    }
    result = determine_completed_by([event])
    assert result is None


# ---------------- Test Flask endpoint (index route) ----------------
# Depending on your app configuration, the index route might exist. If not, expect a 404.

def test_index_endpoint():
    client = app.test_client()
    response = client.get("/")
    # Accept either 200 if defined or 404 if not defined
    assert response.status_code in [200, 404]


# ---------------- Additional Flask endpoint tests ----------------

def test_unknown_endpoint():
    client = app.test_client()
    response = client.get("/nonexistent")
    assert response.status_code == 404


def test_index_post_method():
    client = app.test_client()
    response = client.post("/")
    # Accept 200 if defined, 405 (method not allowed) or 404 if route not available
    assert response.status_code in [200, 405, 404]


def test_load_excel_data_invalid_file(tmp_path):
    non_existent = tmp_path / "nonexistent.xlsx"
    with pytest.raises(Exception):
        load_excel_data(str(non_existent))


# ---------------- Tests for migrate_db ----------------

from app import migrate_db

class FakeConnection:
    def __init__(self, fake_columns):
         self.fake_columns = fake_columns
         self.commands_executed = []
    def execute(self, command):
         self.commands_executed.append(command)
         if command.startswith("PRAGMA table_info"):
              # Simulate returning rows; row[1] is the column name
              return [(0, col) for col in self.fake_columns]
         return None
    def __enter__(self):
         return self
    def __exit__(self, exc_type, exc_val, exc_tb):
         pass

class FakeEngine:
    def __init__(self, fake_columns):
         self.fake_columns = fake_columns
         self.fake_connection = FakeConnection(self.fake_columns)
    def connect(self):
         return self.fake_connection


def test_migrate_db_missing_columns(monkeypatch):
    # Simulate missing all new columns
    fake_columns = ['id']  # missing trip_time, completed_by, coordinate_count, lack_of_accuracy
    fake_engine = FakeEngine(fake_columns)
    monkeypatch.setattr("app.engine", fake_engine)
    migrate_db()
    cmds = fake_engine.fake_connection.commands_executed
    # Expect that 4 ALTER TABLE commands were executed
    alter_cmds = [cmd for cmd in cmds if cmd.startswith("ALTER TABLE trips ADD COLUMN")]
    assert len(alter_cmds) == 4


def test_migrate_db_no_missing_columns(monkeypatch):
    # Simulate that all required columns exist
    fake_columns = ['id', 'trip_time', 'completed_by', 'coordinate_count', 'lack_of_accuracy']
    fake_engine = FakeEngine(fake_columns)
    monkeypatch.setattr("app.engine", fake_engine)
    migrate_db()
    cmds = fake_engine.fake_connection.commands_executed
    alter_cmds = [cmd for cmd in cmds if cmd.startswith("ALTER TABLE trips ADD COLUMN")]
    # Expect no ALTER TABLE commands executed
    assert len(alter_cmds) == 0


def test_migrate_db_exception(monkeypatch, caplog):
    def fake_connect_exception(*args, **kwargs):
         raise Exception("Fake DB error")
    monkeypatch.setattr("app.engine.connect", fake_connect_exception)
    migrate_db()
    # Check that an error log was generated in caplog
    assert "Migration error:" in caplog.text 