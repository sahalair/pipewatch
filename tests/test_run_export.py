"""Tests for pipewatch.run_export."""

import csv
import io
import json
from unittest.mock import patch, MagicMock

import pytest

from pipewatch.run_export import (
    export_run_to_dict,
    export_runs_to_json,
    export_runs_to_csv,
    export_all_runs,
)

RUN_ID = "run-abc-123"

FAKE_RECORD = {
    "run_id": RUN_ID,
    "pipeline": "etl",
    "status": "success",
    "exit_code": 0,
    "started_at": "2024-01-01T10:00:00",
    "finished_at": "2024-01-01T10:05:00",
}

FAKE_METRICS = [
    {"name": "rows", "value": 100, "unit": "count"},
]


def _patch_all(record=None, tags=None, metrics=None, run_ids=None):
    return [
        patch("pipewatch.run_export.load_run_record", return_value=record or FAKE_RECORD),
        patch("pipewatch.run_export.load_tags", return_value={RUN_ID: tags or ["nightly"]}),
        patch("pipewatch.run_export.load_metrics", return_value=metrics or FAKE_METRICS),
        patch("pipewatch.run_export.list_run_records", return_value=[{"run_id": rid} for rid in (run_ids or [RUN_ID])]),
    ]


def test_export_run_to_dict_has_required_keys():
    with _patch_all()[0], _patch_all()[1], _patch_all()[2]:
        with patch("pipewatch.run_export.load_run_record", return_value=FAKE_RECORD), \
             patch("pipewatch.run_export.load_tags", return_value={RUN_ID: ["nightly"]}), \
             patch("pipewatch.run_export.load_metrics", return_value=FAKE_METRICS):
            result = export_run_to_dict(RUN_ID)
    assert "run_id" in result
    assert "pipeline" in result
    assert "status" in result
    assert "tags" in result
    assert "metrics" in result


def test_export_run_to_dict_values():
    with patch("pipewatch.run_export.load_run_record", return_value=FAKE_RECORD), \
         patch("pipewatch.run_export.load_tags", return_value={RUN_ID: ["nightly"]}), \
         patch("pipewatch.run_export.load_metrics", return_value=FAKE_METRICS):
        result = export_run_to_dict(RUN_ID)
    assert result["run_id"] == RUN_ID
    assert result["tags"] == ["nightly"]
    assert len(result["metrics"]) == 1


def test_export_run_to_dict_missing_metrics():
    with patch("pipewatch.run_export.load_run_record", return_value=FAKE_RECORD), \
         patch("pipewatch.run_export.load_tags", return_value={}), \
         patch("pipewatch.run_export.load_metrics", side_effect=FileNotFoundError):
        result = export_run_to_dict(RUN_ID)
    assert result["metrics"] == []
    assert result["tags"] == []


def test_export_runs_to_json_is_valid_json():
    with patch("pipewatch.run_export.load_run_record", return_value=FAKE_RECORD), \
         patch("pipewatch.run_export.load_tags", return_value={}), \
         patch("pipewatch.run_export.load_metrics", return_value=[]):
        output = export_runs_to_json([RUN_ID])
    parsed = json.loads(output)
    assert isinstance(parsed, list)
    assert parsed[0]["run_id"] == RUN_ID


def test_export_runs_to_csv_is_valid_csv():
    with patch("pipewatch.run_export.load_run_record", return_value=FAKE_RECORD), \
         patch("pipewatch.run_export.load_tags", return_value={RUN_ID: ["a", "b"]}), \
         patch("pipewatch.run_export.load_metrics", return_value=FAKE_METRICS):
        output = export_runs_to_csv([RUN_ID])
    reader = csv.DictReader(io.StringIO(output))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["run_id"] == RUN_ID
    assert rows[0]["tags"] == "a;b"
    assert rows[0]["metric_count"] == "1"


def test_export_all_runs_json():
    with patch("pipewatch.run_export.list_run_records", return_value=[{"run_id": RUN_ID}]), \
         patch("pipewatch.run_export.load_run_record", return_value=FAKE_RECORD), \
         patch("pipewatch.run_export.load_tags", return_value={}), \
         patch("pipewatch.run_export.load_metrics", return_value=[]):
        output = export_all_runs(fmt="json")
    assert json.loads(output)[0]["run_id"] == RUN_ID


def test_export_all_runs_csv():
    with patch("pipewatch.run_export.list_run_records", return_value=[{"run_id": RUN_ID}]), \
         patch("pipewatch.run_export.load_run_record", return_value=FAKE_RECORD), \
         patch("pipewatch.run_export.load_tags", return_value={}), \
         patch("pipewatch.run_export.load_metrics", return_value=[]):
        output = export_all_runs(fmt="csv")
    assert RUN_ID in output
    assert "pipeline" in output
