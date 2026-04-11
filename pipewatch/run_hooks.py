"""Run lifecycle hooks — register callbacks for run start/finish events."""

from typing import Callable, Dict, List, Optional

_HOOKS: Dict[str, List[Callable]] = {
    "on_run_start": [],
    "on_run_finish": [],
    "on_run_fail": [],
}


def register_hook(event: str, callback: Callable) -> None:
    """Register a callback for a lifecycle event.

    Supported events: 'on_run_start', 'on_run_finish', 'on_run_fail'.
    """
    if event not in _HOOKS:
        raise ValueError(
            f"Unknown event '{event}'. Must be one of: {list(_HOOKS.keys())}"
        )
    _HOOKS[event].append(callback)


def unregister_hooks(event: Optional[str] = None) -> None:
    """Clear all hooks for a given event, or all events if event is None."""
    if event is None:
        for key in _HOOKS:
            _HOOKS[key].clear()
    else:
        if event not in _HOOKS:
            raise ValueError(f"Unknown event '{event}'.")
        _HOOKS[event].clear()


def fire_hook(event: str, run_record: dict) -> List[str]:
    """Fire all callbacks registered for the given event.

    Returns a list of error messages for any callback that raised.
    """
    if event not in _HOOKS:
        raise ValueError(f"Unknown event '{event}'.")

    errors: List[str] = []
    for callback in _HOOKS[event]:
        try:
            callback(run_record)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{callback.__name__}: {exc}")
    return errors


def list_hooks() -> Dict[str, List[str]]:
    """Return a summary of registered hook names per event."""
    return {
        event: [cb.__name__ for cb in callbacks]
        for event, callbacks in _HOOKS.items()
    }
