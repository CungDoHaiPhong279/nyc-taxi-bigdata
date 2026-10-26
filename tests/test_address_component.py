"""Browser bridge events with synthetic choices; no Streamlit/network required."""

from copy import deepcopy
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest

from nyc_taxi.dashboard.address_component import (
    AddressSelectionError, MAX_EVENT_IDS, new_address_state, process_address_event,
)
from nyc_taxi.dashboard.mapbox_client import MapboxError


CHOICE = {"label": "Synthetic A", "longitude": -73.98, "latitude": 40.75}


def event(state, kind, **values):
    return {"event_id": str(uuid4()), "session_token": state["session_token"], "kind": kind,
            "query": state["query"] or "Synthetic A", "version": state["version"] or 1, **values}


def client():
    result = Mock()
    result.suggest.return_value = [{"mapbox_id": "synthetic-a", "label": "Synthetic A"}]
    result.retrieve.return_value = deepcopy(CHOICE)
    return result


def test_one_character_suggest_and_automatic_retrieve_use_same_uuid_once():
    state, api = new_address_state(), client()
    token = state["session_token"]
    assert UUID(token).version == 4
    search = event(state, "suggest", query="m")
    process_address_event(state, search, api)
    process_address_event(state, search, api)  # persisted component value on rerun
    process_address_event(state, event(state, "suggest"), api)  # another UUID, same query/version
    retrieval = event(state, "retrieve", mapbox_id="synthetic-a")
    process_address_event(state, retrieval, api)
    process_address_event(state, retrieval, api)
    api.suggest.assert_called_once_with("m", token)
    api.retrieve.assert_called_once_with("synthetic-a", token)
    assert state["session_token"] != token  # retrieval ends the billing session
    assert state["selected"] == CHOICE and state["error"] is None and state["status"] == "selected"
    api.retrieve.return_value["latitude"] = 0
    assert state["selected"]["latitude"] == 40.75


def test_search_after_selection_has_new_token_and_clears_previous_coordinates():
    state, api = new_address_state(), client()
    old = state["session_token"]
    process_address_event(state, event(state, "suggest", query="Synthetic A"), api)
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    process_address_event(state, event(state, "edit", query="Synthetic B", version=2), None)
    assert state["selected"] is None and not state["suggestions"] and state["status"] == "idle"
    process_address_event(state, event(state, "suggest"), api)
    assert api.suggest.call_args.args[1] != old
    assert state["selected"] is None and state["retrieved"] is None


def test_browser_coordinates_and_old_select_confirmation_are_never_trusted():
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest", query="Synthetic A"), api)
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a",
                                     selection={**CHOICE, "longitude": -180}), api)
    assert state["selected"] == CHOICE
    process_address_event(state, event(state, "select", selection={**CHOICE, "longitude": -180}), api)
    assert state["selected"] == CHOICE


@pytest.mark.parametrize("coordinates", [(float("nan"), 40), (-73, float("inf")), (181, 40),
                                         (-73, -91), (True, 40), ("-73", 40)])
def test_backend_validates_retrieve_coordinates_even_with_injected_client(coordinates):
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest"), api)
    api.retrieve.return_value = {"label": "Synthetic invalid", "longitude": coordinates[0], "latitude": coordinates[1]}
    old = state["session_token"]
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    assert state["selected"] is None and state["error"] and state["session_token"] != old


@pytest.mark.parametrize("kind", ["suggest", "retrieve"])
def test_failed_http_event_never_retries_or_logs_on_rerun(kind, caplog):
    state, api = new_address_state(), client()
    if kind == "retrieve":
        process_address_event(state, event(state, "suggest"), api)
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


@pytest.mark.parametrize("mode", ["expired", "count"])
def test_expired_or_fifty_suggest_session_rotates_before_search(mode, monkeypatch):
    state, api = new_address_state(), client()
    if mode == "expired":
        now = state["started_at"] + 181
        monkeypatch.setattr("nyc_taxi.dashboard.address_component.monotonic", lambda: now)
    else:
        state["suggest_count"] = 50
    old = state["session_token"]
    process_address_event(state, event(state, "suggest", query="Synthetic A"), api)
    assert api.suggest.call_args.args[1] != old
    assert state["suggest_count"] == 1 and state["status"] == "results"


def test_stale_or_unrecognized_candidate_does_not_retrieve():
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest"), api)
    stale = event(state, "retrieve", mapbox_id="synthetic-a")
    stale["session_token"] = str(uuid4())
    process_address_event(state, stale, api)
    process_address_event(state, event(state, "retrieve", mapbox_id="not-suggested"), api)
    api.retrieve.assert_not_called()


@pytest.mark.parametrize("identifier", [None, [], {}, 123])
def test_malformed_candidate_id_is_ignored_without_http_or_exception(identifier):
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest"), api)
    process_address_event(state, event(state, "retrieve", mapbox_id=identifier), api)
    api.retrieve.assert_not_called()


@pytest.mark.parametrize("mode", ["expired", "count"])
def test_retrieve_cannot_use_expired_or_completed_session(mode, monkeypatch):
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest"), api)
    if mode == "expired":
        now = state["started_at"] + 181
        monkeypatch.setattr("nyc_taxi.dashboard.address_component.monotonic", lambda: now)
    else:
        state["suggest_count"] = 50
    old_token = state["session_token"]
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    api.retrieve.assert_not_called()
    assert state["session_token"] != old_token and state["error"]


def test_no_event_no_work_and_invalid_event_id_rejected():
    state, api = new_address_state(), client()
    process_address_event(state, None, api)
    with pytest.raises(AddressSelectionError):
        process_address_event(state, {"event_id": "invalid"}, api)
    api.suggest.assert_not_called()


def test_empty_edit_clears_choice_and_suggestions_without_network():
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest"), api)
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    api.reset_mock()
    process_address_event(state, event(state, "edit", query="", version=2), None)
    process_address_event(state, event(state, "suggest", query=""), api)
    assert state["query"] == "" and state["selected"] is None and not state["suggestions"]
    assert state["error"] is None and state["status"] == "idle"
    api.suggest.assert_not_called()
    api.retrieve.assert_not_called()


def test_suggest_after_edit_same_version_works_once_and_preserves_raw_query():
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "edit", query=" m "), None)
    process_address_event(state, event(state, "suggest"), api)
    process_address_event(state, event(state, "edit"), None)
    process_address_event(state, event(state, "suggest"), api)
    assert state["query"] == " m " and state["suggestions_query"] == " m "
    assert state["status"] == "results" and state["suggestions_version"] == 1
    api.suggest.assert_called_once()


@pytest.mark.parametrize("late_failure", [False, True])
def test_out_of_order_suggest_result_or_error_cannot_overwrite_newer_query(late_failure):
    state, api = new_address_state(), client()
    newer = [{"mapbox_id": "synthetic-b", "label": "Synthetic B"}]

    def suggest(query, token):
        if query == "m":
            process_address_event(state, event(state, "edit", query="met", version=2), None)
            process_address_event(state, event(state, "suggest"), api)
            if late_failure:
                raise MapboxError("SYNTHETIC_PRIVATE_RESPONSE")
            return [{"mapbox_id": "synthetic-old", "label": "Old result"}]
        return newer

    api.suggest.side_effect = suggest
    process_address_event(state, event(state, "suggest", query="m"), api)
    assert state["query"] == "met" and state["version"] == state["suggestions_version"] == 2
    assert state["suggestions"] == newer and state["error"] is None and state["status"] == "results"
    process_address_event(state, event(state, "retrieve", query="m", version=1, mapbox_id="synthetic-old"), api)
    api.retrieve.assert_not_called()


def test_newer_edit_during_retrieve_cannot_receive_old_selected_coordinates():
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest", query="m"), api)
    old = state["session_token"]

    def retrieve(identifier, token):
        assert token == old
        process_address_event(state, event(state, "edit", query="met", version=2), None)
        process_address_event(state, event(state, "suggest"), api)
        return deepcopy(CHOICE)

    api.retrieve.side_effect = retrieve
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    assert state["query"] == "met" and state["version"] == 2 and state["selected"] is None
    assert state["status"] == "results" and state["error"] is None
    assert state["session_token"] != old and api.suggest.call_args.args[1] == state["session_token"]


def test_stale_events_and_tampered_same_version_do_not_change_selected_result():
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest", query="met", version=3), api)
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    before = deepcopy(state["selected"])
    process_address_event(state, event(state, "edit", query="m", version=1), None)
    process_address_event(state, event(state, "edit", query="changed", version=3), None)
    process_address_event(state, event(state, "retrieve", query="changed", version=4, mapbox_id="synthetic-a"), api)
    assert state["selected"] == before and state["query"] == "met" and state["error"] is None
    api.retrieve.assert_called_once()


def test_last_128_event_ids_are_bounded_without_a_permanent_interaction_cap():
    state, api = new_address_state(), client()
    first = event(state, "suggest", query="m")
    process_address_event(state, first, api)
    for version in range(2, MAX_EVENT_IDS + 20):
        process_address_event(state, event(state, "edit", query="met", version=version), None)
    assert len(state["events"]) == MAX_EVENT_IDS and first["event_id"] not in state["events"]
    process_address_event(state, first, api)  # event eviction does not allow replaying an old version
    process_address_event(state, event(state, "suggest"), api)
    process_address_event(state, event(state, "suggest"), api)
    assert api.suggest.call_count == 2 and state["status"] == "results"


def test_pickup_and_dropoff_sessions_versions_and_selection_are_independent():
    pickup, dropoff, api = new_address_state(), new_address_state(), client()
    assert pickup["session_token"] != dropoff["session_token"]
    for state in (pickup, dropoff):
        process_address_event(state, event(state, "suggest", query="m"), api)
    process_address_event(pickup, event(pickup, "retrieve", mapbox_id="synthetic-a"), api)
    assert pickup["selected"] == CHOICE and dropoff["selected"] is None
    assert dropoff["status"] == "results" and dropoff["suggestions"]
    process_address_event(dropoff, event(dropoff, "edit", query="me", version=2), None)
    assert pickup["selected"] == CHOICE


@pytest.mark.parametrize("version", [None, 0, -1, True, "1", 2**31])
def test_invalid_versions_cannot_start_http(version):
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest", version=version), api)
    api.suggest.assert_not_called()


@pytest.mark.parametrize("query", [None, "m\n", "m\x7f", "m\x85", "m" * 251])
def test_invalid_queries_never_reach_http_or_become_selected(query):
    state, api = new_address_state(), client()
    process_address_event(state, event(state, "suggest", query=query), api)
    api.suggest.assert_not_called()
    assert state["selected"] is None and state["error"]


def test_empty_results_are_an_explicit_state_and_cannot_be_retrieved():
    state, api = new_address_state(), client()
    api.suggest.return_value = []
    process_address_event(state, event(state, "suggest", query="m"), api)
    assert state["status"] == "empty" and state["suggestions_version"] == 1
    process_address_event(state, event(state, "retrieve", mapbox_id="synthetic-a"), api)
    api.retrieve.assert_not_called()


def test_invalid_newer_edit_invalidates_an_older_inflight_suggest_response():
    state, api = new_address_state(), client()

    def suggest(query, token):
        process_address_event(state, event(state, "edit", query="m\n", version=2), None)
        return [{"mapbox_id": "old", "label": "Old result"}]

    api.suggest.side_effect = suggest
    process_address_event(state, event(state, "suggest", query="m"), api)
    assert state["version"] == 2 and state["status"] == "error" and not state["suggestions"]


def test_request_counters_measure_guarded_attempts_without_rerun_or_clear_increments():
    state, api = new_address_state(), client()
    search = event(state, "suggest", query="m")
    process_address_event(state, search, api)
    process_address_event(state, search, api)
    process_address_event(state, event(state, "suggest"), api)
    process_address_event(state, event(state, "retrieve", mapbox_id="not-a-result"), api)
    assert state["request_counts"] == {"suggest": 1, "retrieve": 0}
    retrieval = event(state, "retrieve", mapbox_id="synthetic-a")
    process_address_event(state, retrieval, api)
    process_address_event(state, retrieval, api)
    process_address_event(state, event(state, "edit", query="", version=2), None)
    assert state["request_counts"] == {"suggest": 1, "retrieve": 1}
    api.suggest.side_effect = MapboxError("Synthetic failed attempt")
    process_address_event(state, event(state, "suggest", query="met", version=3), api)
    process_address_event(state, event(state, "suggest"), api)
    assert state["request_counts"] == {"suggest": 2, "retrieve": 1}
