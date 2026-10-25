"""Browser bridge events with synthetic choices; no Streamlit/network required."""

from copy import deepcopy
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest

from nyc_taxi.dashboard.address_component import (
    AddressSelectionError, new_address_state, process_address_event,
)
from nyc_taxi.dashboard.mapbox_client import MapboxError


CHOICE = {"label": "Synthetic A", "longitude": -73.98, "latitude": 40.75}


def event(state, kind, **values):
    return {"event_id": str(uuid4()), "session_token": state["session_token"], "kind": kind, **values}


def client():
    result = Mock()
    result.suggest.return_value = [{"mapbox_id": "synthetic-a", "label": "Synthetic A"}]
    result.retrieve.return_value = deepcopy(CHOICE)
    return result


def test_browser_suggest_retrieve_coordinates_roundtrip_uses_same_uuid_once():
    state, api = new_address_state(), client()
    token = state["session_token"]
    assert UUID(token).version == 4
    search = event(state, "suggest", query="Synthetic A")
    process_address_event(state, search, api)
    process_address_event(state, search, api)  # persisted component value on rerun
    retrieval = event(state, "retrieve", mapbox_id="synthetic-a")
    process_address_event(state, retrieval, api)
    process_address_event(state, retrieval, api)
    api.suggest.assert_called_once_with("Synthetic A", token)
    api.retrieve.assert_called_once_with("synthetic-a", token)
    assert state["session_token"] != token  # retrieval ends the billing session
    selection = event(state, "select", selection=deepcopy(CHOICE))
    process_address_event(state, selection, api)
    process_address_event(state, selection, api)
    assert state["selected"] == CHOICE and state["error"] is None
    selection["selection"]["latitude"] = 0
    assert state["selected"]["latitude"] == 40.75


def test_search_after_retrieve_without_confirmation_has_new_token():
    state, api = new_address_state(), client()
    old = state["session_token"]
    process_address_event(state, event(state, "suggest", query="Synthetic A"), api)
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    process_address_event(state, event(state, "suggest", query="Synthetic B"), api)
    assert api.suggest.call_args.args[1] != old
    assert state["selected"] is None and state["retrieved"] is None


def test_tampered_coordinates_cannot_replace_retrieved_selection():
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest", query="Synthetic A"), api)
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    process_address_event(state, event(state, "select", selection={**CHOICE, "longitude": -180}), api)
    assert state["selected"] is None and state["error"]


@pytest.mark.parametrize("kind", ["suggest", "retrieve"])
def test_failed_http_event_never_retries_or_logs_on_rerun(kind, caplog):
    state, api = new_address_state(), client()
    state["suggestions"] = api.suggest.return_value
    getattr(api, kind).side_effect = MapboxError("SYNTHETIC_SECRET_ADDRESS")
    action = event(state, kind, query="Synthetic A", mapbox_id="synthetic-a")
    old_token = state["session_token"]
    process_address_event(state, action, api)
    process_address_event(state, action, api)
    assert getattr(api, kind).call_count == 1
    assert state["selected"] is None and state["error"]
    assert "SYNTHETIC_SECRET_ADDRESS" not in state["error"] + caplog.text
    if kind == "retrieve":
        assert state["session_token"] != old_token


def test_expired_or_fifty_suggest_session_rotates_before_search(monkeypatch):
    for mode in ("expired", "count"):
        state, api = new_address_state(), client()
        if mode == "expired":
            monkeypatch.setattr("nyc_taxi.dashboard.address_component.monotonic", lambda: state["started_at"] + 181)
        else:
            state["suggest_count"] = 50
        old = state["session_token"]
        process_address_event(state, event(state, "suggest", query="Synthetic A"), api)
        assert api.suggest.call_args.args[1] != old


def test_stale_or_unrecognized_candidate_does_not_retrieve():
    state, api = new_address_state(), client()
    stale = event(state, "retrieve", mapbox_id="synthetic-a")
    stale["session_token"] = str(uuid4())
    process_address_event(state, stale, api)
    process_address_event(state, event(state, "retrieve", mapbox_id="not-suggested"), api)
    api.retrieve.assert_not_called()


@pytest.mark.parametrize("mode", ["expired", "count"])
def test_retrieve_cannot_use_expired_or_completed_session(mode, monkeypatch):
    state, api = new_address_state(), client()
    state["suggestions"] = api.suggest.return_value
    if mode == "expired":
        now = state["started_at"] + 181
        monkeypatch.setattr("nyc_taxi.dashboard.address_component.monotonic", lambda: now)
    else:
        state["suggest_count"] = 50
    old_token = state["session_token"]
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    api.retrieve.assert_not_called()
    assert state["session_token"] != old_token and state["error"]


def test_no_event_no_work_and_invalid_or_full_session_rejected():
    state, api = new_address_state(), client()
    process_address_event(state, None, api)
    with pytest.raises(AddressSelectionError):
        process_address_event(state, {"event_id": "invalid"}, api)
    state["events"] = [str(uuid4()) for _ in range(64)]
    with pytest.raises(AddressSelectionError):
        process_address_event(state, event(state, "suggest", query="Synthetic A"), api)
    api.suggest.assert_not_called()
