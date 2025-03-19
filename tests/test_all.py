import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import os
import json
import pandas as pd
import pytest
from io import StringIO

import exportmix
import consolidatemixpanel


def simulate_requests_success(monkeypatch):
    class FakeResponse:
        def __init__(self, text, status_code):
            self.text = text
            self.status_code = status_code
    
    def fake_get(*args, **kwargs):
        ndjson = (
            '{"properties": {"time": 1680000000, "tripId": "trip_123", "model": "220733SFG"}, "event": "test_event"}\n'
            '{"properties": {"time": 1680003600, "tripId": "trip_456", "model": "23028RNCAG"}, "event": "test_event"}\n'
        )
        return FakeResponse(ndjson, 200)
    monkeypatch.setattr(exportmix.requests, 'get', fake_get)


def simulate_requests_failure(monkeypatch):
    class FakeResponse:
        def __init__(self, text, status_code):
            self.text = text
            self.status_code = status_code
    
    def fake_get(*args, **kwargs):
        return FakeResponse("Not Found", 404)
    monkeypatch.setattr(exportmix.requests, 'get', fake_get)


def create_sample_excel(file_path, include_extra_columns=False):
    # Create a sample Excel with required columns: 'tripId', 'time', 'model'
    data = {
         'tripId': ['trip_123', 'trip_123', 'trip_456'],
         'time': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-01 09:00:00'), pd.Timestamp('2023-01-02 12:00:00')],
         'model': ['220733SFG', '220733SFG', '23028RNCAG']
    }
    if include_extra_columns:
         data['app_version'] = ['1.0', '1.0', '1.1']
    df = pd.DataFrame(data)
    df.to_excel(file_path, index=False)


def test_export_data_success(monkeypatch, tmp_path, capsys):
    # Use a temporary directory to avoid polluting local workspace
    os.chdir(tmp_path)
    simulate_requests_success(monkeypatch)
    start_date = '2023-01-01'
    end_date = '2023-01-02'
    exportmix.export_data(start_date, end_date)
    captured = capsys.readouterr()
    assert "Data successfully saved to mixpanel_export.xlsx" in captured.out
    assert os.path.exists("mixpanel_export.xlsx")
    df = pd.read_excel("mixpanel_export.xlsx")
    assert 'tripId' in df.columns
    assert 'time' in df.columns
    assert pd.api.types.is_datetime64_any_dtype(df['time'])


def test_export_data_failure(monkeypatch, tmp_path, capsys):
    os.chdir(tmp_path)
    simulate_requests_failure(monkeypatch)
    start_date = '2023-01-01'
    end_date = '2023-01-02'
    exportmix.export_data(start_date, end_date)
    captured = capsys.readouterr()
    assert "Failed to export data:" in captured.out
    assert not os.path.exists("mixpanel_export.xlsx")


def test_export_data_edge_invalid_json(monkeypatch, tmp_path, capsys):
    os.chdir(tmp_path)
    class FakeResponse:
        def __init__(self, text, status_code):
            self.text = text
            self.status_code = status_code
    def fake_get(*args, **kwargs):
        ndjson = (
            '{"properties": {"time": 1680000000, "tripId": "trip_invalid", "model": "Model C"}, "event": "test_event"}\n'
            'not a json line\n'
        )
        return FakeResponse(ndjson, 200)
    monkeypatch.setattr(exportmix.requests, 'get', fake_get)
    start_date = '2023-01-01'
    end_date = '2023-01-02'
    with pytest.raises(json.JSONDecodeError):
        exportmix.export_data(start_date, end_date)


def test_consolidate_data_success(monkeypatch, tmp_path, capsys):
    # Create a temporary mixpanel_export.xlsx file
    tmp_excel = tmp_path / "mixpanel_export.xlsx"
    create_sample_excel(tmp_excel, include_extra_columns=True)
    os.chdir(tmp_path)
    # Remove output directory if exists
    output_dir = tmp_path / "data"
    if output_dir.exists():
         import shutil
         shutil.rmtree(str(output_dir))
    consolidatemixpanel.consolidate_data()
    captured = capsys.readouterr()
    assert "Consolidated file saved as 'data/data.xlsx'" in captured.out
    output_file = tmp_path / "data" / "data.xlsx"
    assert output_file.exists()
    df = pd.read_excel(output_file)
    # Check that duplicate tripIds have been removed
    assert df['tripId'].nunique() == 2


def test_consolidate_data_failure(tmp_path, capsys):
    os.chdir(tmp_path)
    # Ensure mixpanel_export.xlsx does not exist
    if os.path.exists("mixpanel_export.xlsx"):
         os.remove("mixpanel_export.xlsx")
    with pytest.raises(SystemExit):
         consolidatemixpanel.consolidate_data()
    captured = capsys.readouterr()
    assert "Failed to consolidate data:" in captured.out


def test_consolidate_data_edge_empty(tmp_path, capsys):
    # Create an empty Excel file with just headers
    data = {'tripId': [], 'time': [], 'model': []}
    tmp_excel = tmp_path / "mixpanel_export.xlsx"
    pd.DataFrame(data).to_excel(tmp_excel, index=False)
    os.chdir(tmp_path)
    consolidatemixpanel.consolidate_data()
    captured = capsys.readouterr()
    output_file = tmp_path / "data" / "data.xlsx"
    df = pd.read_excel(output_file)
    assert df.empty 


def test_mixpanel_dummy():
    import mixpanel
    assert mixpanel is not None 