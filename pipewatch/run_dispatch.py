"""run_dispatch.py — Route pipeline run events to registered handlers based on status or pipeline name."""

from typing import Callable, Dict, List, Optional

# Registry: maps (pipeline, status) -> list of handler callables
# Use "*" as wildcard for pipeline or status
_dispatch_table: Dict[tuple, List[Callable]] = {}


def register_handler(
    handler: Callable,
    pipeline: str = "*",
    status: str = "*",
) -> None:
    """Register a handler for a given pipeline and/or status.

    Use '*' as a wildcard to match any pipeline or status.
    """
    key = (pipeline, status)
    if key not in _dispatch_table:
        _dispatch_table[key] = []
    if handler not in _dispatch_table[key]:
        _dispatch_table[key].append(handler)


def unregister_handler(
    handler: Callable,
    pipeline: str = "*",
    status: str = "*",
) -> bool:
    """Remove a handler. Returns True if it was found and removed."""
    key = (pipeline, status)
    handlers = _dispatch_table.get(key, [])
    if handler in handlers:
        handlers.remove(handler)
        return True
    return False


def clear_handlers() -> None:
    """Remove all registered handlers (useful for testing)."""
    _dispatch_table.clear()


def _matching_keys(pipeline: str, status: str) -> List[tuple]:
    """Return all registry keys that match a given pipeline and status."""
    return [
        (p, s)
        for (p, s) in _dispatch_table
        if (p == pipeline or p == "*") and (s == status or s == "*")
    ]


def dispatch(run: dict) -> List[str]:
    """Dispatch a run record to all matching handlers.

    Returns a list of handler names that were called.
    """
    pipeline = run.get("pipeline", "")
    status = run.get("status", "")
    called = []
    seen = set()
    for key in _matching_keys(pipeline, status):
        for handler in _dispatch_table.get(key, []):
            hid = id(handler)
            if hid not in seen:
                handler(run)
                called.append(getattr(handler, "__name__", repr(handler)))
                seen.add(hid)
    return called


def list_handlers(pipeline: Optional[str] = None, status: Optional[str] = None) -> List[dict]:
    """List all registered handlers, optionally filtered."""
    results = []
    for (p, s), handlers in _dispatch_table.items():
        if pipeline and p != pipeline and p != "*":
            continue
        if status and s != status and s != "*":
            continue
        for h in handlers:
            results.append({
                "pipeline": p,
                "status": s,
                "handler": getattr(h, "__name__", repr(h)),
            })
    return results
