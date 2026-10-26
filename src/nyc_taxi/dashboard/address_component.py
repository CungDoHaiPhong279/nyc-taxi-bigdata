"""Versioned autocomplete bridge; all address/session state stays in memory."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from time import monotonic
from uuid import UUID, uuid4

from nyc_taxi.dashboard.mapbox_client import MapboxError, validate_coordinates


MAX_EVENT_IDS = 128
MAX_SUGGESTS = 50
SESSION_SECONDS = 180
MAX_VERSION = 2**31 - 1
_PUBLIC_ERROR = "Không thể chọn địa chỉ. Kiểm tra kết nối/cấu hình hoặc tìm lại địa chỉ."


class AddressSelectionError(RuntimeError):
    """Safe bridge error; never echo an upstream response or user query."""


def new_address_state() -> dict:
    return {
        "session_token": str(uuid4()), "events": [], "suggestions": [],
        "started_at": monotonic(), "suggest_count": 0,
        "query": "", "version": 0, "suggested_version": 0,
        "suggestions_query": "", "suggestions_version": 0,
        "request_counts": {"suggest": 0, "retrieve": 0},
        "retrieved": None, "selected": None, "error": None, "status": "idle",
    }


def _uuid(value: object) -> bool:
    try:
        return isinstance(value, str) and UUID(value).version == 4 and str(UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def _query(value: object) -> str:
    if (
        not isinstance(value, str) or len(value) > 250
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise AddressSelectionError("Nội dung tìm địa chỉ không hợp lệ.")
    return value.strip()


def _rotate(state: dict) -> None:
    state.update(session_token=str(uuid4()), started_at=monotonic(), suggest_count=0)


def _clear_choice(state: dict) -> None:
    state.update(suggestions=[], suggestions_query="", suggestions_version=0,
                 retrieved=None, selected=None, error=None, status="idle")


def _snapshot(state: dict) -> tuple[int, str, str]:
    return state["version"], state["query"].strip(), state["session_token"]


def _current(state: dict, snapshot: tuple[int, str, str]) -> bool:
    return _snapshot(state) == snapshot


def _failure(state: dict) -> None:
    _clear_choice(state)
    state.update(error=_PUBLIC_ERROR, status="error")


def _retrieve(state: dict, event: dict, client: object) -> None:
    if state["suggest_count"] >= MAX_SUGGESTS or monotonic() - state["started_at"] >= SESSION_SECONDS:
        _rotate(state)
        _failure(state)
        return
    identifier = event.get("mapbox_id")
    if (
        state["status"] != "results"
        or state["suggestions_version"] != state["version"]
        or state["suggestions_query"].strip() != state["query"].strip()
        or not isinstance(identifier, str)
        or identifier not in {row["mapbox_id"] for row in state["suggestions"]}
    ):
        # A stale/forged candidate never causes a retrieve or changes a selection.
        return
    snapshot = _snapshot(state)
    state.update(suggestions=[], status="retrieving", selected=None, retrieved=None, error=None)
    candidate = None
    current = False
    try:
        state["request_counts"]["retrieve"] += 1
        candidate = client.retrieve(event["mapbox_id"], snapshot[2])
        if not isinstance(candidate, dict) or not isinstance(candidate.get("label"), str) or not candidate["label"].strip():
            raise AddressSelectionError("Địa chỉ trả về không hợp lệ.")
        longitude, latitude = validate_coordinates(candidate.get("longitude"), candidate.get("latitude"))
        # Coordinates are exclusively the server-side retrieve result, never a
        # browser selection payload. Whitelist fields before saving them.
        candidate = {"label": candidate["label"], "longitude": longitude, "latitude": latitude}
        current = _current(state, snapshot)
    except Exception:
        current = _current(state, snapshot)
        if current:
            _failure(state)
        candidate = None
    finally:
        # Retrieve ends a billing session even if the request fails. A newer
        # edit may already have moved an interrupted retrieve to a new session.
        if state["session_token"] == snapshot[2]:
            _rotate(state)
    if current and candidate is not None:
        state.update(selected=deepcopy(candidate), status="selected", error=None,
                     suggestions_query="", suggestions_version=0)


def process_address_event(state: dict, event: object, client: object) -> None:
    """Consume edit/suggest/retrieve once; reject stale versions and responses.

    A query version is monotonic for the component lifetime. The bounded event
    window and last suggested version prevent both Streamlit replay and repeated
    HTTP requests even when a browser generates another event UUID.
    """
    if event is None:
        return
    if not isinstance(event, dict) or not _uuid(event.get("event_id")):
        raise AddressSelectionError("Sự kiện chọn địa chỉ không hợp lệ.")
    if event["event_id"] in state["events"]:
        return
    state["events"].append(event["event_id"])
    del state["events"][:-MAX_EVENT_IDS]
    if event.get("session_token") != state["session_token"]:
        return
    version = event.get("version")
    if type(version) is not int or not 1 <= version <= MAX_VERSION:
        return
    if version < state["version"]:
        return
    kind = event.get("kind")
    if kind not in {"edit", "suggest", "retrieve"}:
        return
    try:
        normalized = _query(event.get("query"))
    except AddressSelectionError:
        if version > state["version"] and kind in {"edit", "suggest"}:
            if state["status"] == "retrieving":
                _rotate(state)
            state.update(query="", version=version)
            _failure(state)
        return
    if version == state["version"] and normalized != state["query"].strip():
        return
    if version > state["version"]:
        if kind == "retrieve":
            return  # a candidate cannot introduce a query the server never saw
        if state["status"] == "retrieving":
            _rotate(state)
        _clear_choice(state)
        state.update(query=event["query"], version=version)
    if kind == "edit":
        return  # no network; duplicate edits cannot clear newer results
    if kind == "retrieve":
        _retrieve(state, event, client)
        return
    if not normalized or version <= state["suggested_version"]:
        return
    state["suggested_version"] = version
    if state["suggest_count"] >= MAX_SUGGESTS or monotonic() - state["started_at"] >= SESSION_SECONDS:
        _rotate(state)
    _clear_choice(state)
    state.update(status="searching", suggest_count=state["suggest_count"] + 1)
    snapshot = _snapshot(state)
    try:
        state["request_counts"]["suggest"] += 1
        suggestions = client.suggest(event["query"], snapshot[2])
        if (
            not isinstance(suggestions, list) or len(suggestions) > 5
            or any(not isinstance(row, dict) or not isinstance(row.get("mapbox_id"), str)
                   or not row["mapbox_id"] or not isinstance(row.get("label"), str)
                   or not row["label"].strip() for row in suggestions)
        ):
            raise MapboxError("Danh sách gợi ý không hợp lệ.")
        if _current(state, snapshot):
            state.update(suggestions=deepcopy(suggestions), suggestions_query=state["query"],
                         suggestions_version=version, status="results" if suggestions else "empty")
    except Exception:
        # Do not expose an upstream exception or poison a newer query's state.
        if _current(state, snapshot):
            _failure(state)


def address_picker(state: dict, *, label: str, key: str, online: bool = False):
    """Browser frontend receives session UUID/state, never the Mapbox token."""
    import streamlit.components.v1 as components

    component = components.declare_component(
        "nyc_address_picker", path=str(Path(__file__).with_name("address_frontend")),
    )
    return component(
        label=label, online=online is True, session_token=state["session_token"],
        query=state["query"], version=state["version"],
        suggestions=state["suggestions"], suggestions_query=state["suggestions_query"],
        suggestions_version=state["suggestions_version"], status=state["status"],
        error=state["error"], selected=state["selected"],
        request_counts=dict(state["request_counts"]), default=None, key=key,
    )
