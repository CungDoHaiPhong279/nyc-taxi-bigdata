"""Local Streamlit component bridge; address state is session memory only."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from time import monotonic
from uuid import UUID, uuid4

from nyc_taxi.dashboard.mapbox_client import MapboxError, validate_coordinates


class AddressSelectionError(RuntimeError):
    """Safe bridge error; never echo an upstream response or user query."""


def new_address_state() -> dict:
    return {"session_token": str(uuid4()), "events": [], "suggestions": [],
            "started_at": monotonic(), "suggest_count": 0,
            "retrieved": None, "selected": None, "error": None}


def _uuid(value: object) -> bool:
    try:
        return isinstance(value, str) and UUID(value).version == 4 and str(UUID(value)) == value
    except (ValueError, AttributeError):
        return False


def process_address_event(state: dict, event: object, client: object) -> None:
    """Consume a browser event once, including failed HTTP attempts.

    Search/retrieve share the server-generated UUID. A separate confirmation
    sends the selected coordinates back from the browser; Python compares them
    with the retrieved candidate before accepting the selection.
    """
    if event is None:
        return
    if not isinstance(event, dict) or not _uuid(event.get("event_id")):
        raise AddressSelectionError("Sự kiện chọn địa chỉ không hợp lệ.")
    event_id = event["event_id"]
    if event_id in state["events"]:
        return
    if len(state["events"]) >= 64:
        raise AddressSelectionError("Phiên tìm kiếm đã đủ số thao tác. Hãy mở phiên chọn địa chỉ mới.")
    state["events"].append(event_id)
    if event.get("session_token") != state["session_token"]:
        return  # stale iframe event after a successful selection, no HTTP replay
    state["error"] = None
    kind = event.get("kind")
    try:
        if kind == "suggest":
            if state["suggest_count"] >= 50 or monotonic() - state["started_at"] >= 180:
                state.update(session_token=str(uuid4()), started_at=monotonic(), suggest_count=0)
            state.update(suggestions=[], retrieved=None, selected=None)
            state["suggest_count"] += 1
            state["suggestions"] = client.suggest(event.get("query"), state["session_token"])
        elif kind == "retrieve":
            if state["suggest_count"] >= 50 or monotonic() - state["started_at"] >= 180:
                state.update(session_token=str(uuid4()), started_at=monotonic(), suggest_count=0,
                             suggestions=[], retrieved=None, selected=None)
                raise AddressSelectionError("Phiên tìm kiếm đã hết hạn. Hãy tìm địa chỉ lại.")
            identifier = event.get("mapbox_id")
            if identifier not in {row["mapbox_id"] for row in state["suggestions"]}:
                raise AddressSelectionError("Hãy chọn một kết quả của phiên tìm kiếm hiện tại.")
            # One retrieval ends a billable search session. Retrying requires a
            # new search/session rather than replaying this retrieve on rerun.
            state["suggestions"] = []
            state["retrieved"] = None
            state["selected"] = None
            try:
                state["retrieved"] = client.retrieve(identifier, state["session_token"])
            finally:
                state.update(session_token=str(uuid4()), started_at=monotonic(), suggest_count=0)
        elif kind == "select":
            selected = event.get("selection")
            if state["retrieved"] is None or selected != state["retrieved"]:
                raise AddressSelectionError("Lựa chọn không khớp địa chỉ vừa xác nhận.")
            validate_coordinates(selected.get("longitude"), selected.get("latitude"))
            state["selected"] = deepcopy(selected)
            state["retrieved"] = None
        else:
            raise AddressSelectionError("Thao tác chọn địa chỉ không hợp lệ.")
    except (MapboxError, AddressSelectionError):
        state["error"] = "Không thể chọn địa chỉ. Kiểm tra kết nối/cấu hình hoặc tìm lại địa chỉ."
        state["selected"] = None
        # Expired/failed retrieve must not re-use the finished search session.
        if kind == "retrieve":
            state["session_token"] = str(uuid4())
    except Exception:
        state["error"] = "Không thể xử lý lựa chọn địa chỉ. Hãy tìm lại địa chỉ."
        state["selected"] = None


def address_picker(state: dict, *, label: str, key: str):
    """Browser frontend contains no token, external scripts or direct API calls."""
    import streamlit.components.v1 as components

    component = components.declare_component(
        "nyc_address_picker", path=str(Path(__file__).with_name("address_frontend")),
    )
    return component(label=label, session_token=state["session_token"],
                     suggestions=state["suggestions"], retrieved=state["retrieved"],
                     selected=state["selected"], default=None, key=key)
