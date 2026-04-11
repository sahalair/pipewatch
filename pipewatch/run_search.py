"""Full-text and field search across run records and their metadata."""

from __future__ import annotations

from typing import Any

from pipewatch.run_logger import list_run_records
from pipewatch.run_annotations import get_annotations
from pipewatch.run_notes import get_notes
from pipewatch.tag_manager import load_tags


def _run_matches_query(run: dict[str, Any], query: str) -> bool:
    """Return True if the query string appears in any searchable field of the run."""
    q = query.lower()
    searchable = [
        run.get("run_id", ""),
        run.get("pipeline", ""),
        run.get("status", ""),
        run.get("started_at", ""),
        run.get("finished_at", ""),
    ]
    return any(q in str(v).lower() for v in searchable if v)


def _run_matches_field(run: dict[str, Any], field: str, value: str) -> bool:
    """Return True if run[field] matches value (case-insensitive substring)."""
    actual = run.get(field, "")
    return value.lower() in str(actual).lower()


def search_runs(
    query: str | None = None,
    field: str | None = None,
    field_value: str | None = None,
    search_notes: bool = False,
    search_annotations: bool = False,
    data_dir: str = ".pipewatch",
) -> list[dict[str, Any]]:
    """Search run records by query string, field match, notes, or annotations.

    Args:
        query: Free-text string matched against common run fields.
        field: A specific run record field to match against.
        field_value: Value to match for the given field.
        search_notes: Include runs whose notes contain the query.
        search_annotations: Include runs whose annotation values contain the query.
        data_dir: Root directory for pipewatch data.

    Returns:
        List of matching run record dicts.
    """
    all_runs = list_run_records(data_dir=data_dir)
    results = []

    for run in all_runs:
        run_id = run.get("run_id", "")
        matched = False

        if query:
            if _run_matches_query(run, query):
                matched = True

            if not matched and search_notes:
                notes = get_notes(run_id, data_dir=data_dir)
                if any(query.lower() in n.get("text", "").lower() for n in notes):
                    matched = True

            if not matched and search_annotations:
                annotations = get_annotations(run_id, data_dir=data_dir)
                if any(
                    query.lower() in str(v).lower()
                    for v in annotations.values()
                ):
                    matched = True

        if field and field_value:
            if _run_matches_field(run, field, field_value):
                matched = True

        if matched:
            results.append(run)

    return results


def format_search_results(results: list[dict[str, Any]]) -> str:
    """Format a list of search result run records as a human-readable string."""
    if not results:
        return "No matching runs found."
    lines = []
    for run in results:
        status = run.get("status", "unknown")
        pipeline = run.get("pipeline", "?")
        started = run.get("started_at", "?")
        lines.append(f"  [{run['run_id']}] pipeline={pipeline} status={status} started={started}")
    return "\n".join(lines)
