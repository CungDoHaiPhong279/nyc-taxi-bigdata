"""Interrupted-render completion, using bounded threads and synthetic transport.

No Streamlit, provider, browser, credentials or real locations are required.
These transport counters represent the injected mock, not outbound HTTP proof.
"""

from copy import deepcopy
from threading import Event, Thread
from time import monotonic
from unittest.mock import Mock
from uuid import uuid4

import pytest

from nyc_taxi.dashboard.address_component import (
    new_address_state, process_address_event, wait_for_address_request,
)
from nyc_taxi.dashboard.mapbox_client import MapboxError


SUGGESTIONS = [{"mapbox_id": "synthetic-a", "label": "Synthetic option A"}]
SELECTION = {"label": "Synthetic option A", "longitude": -73.98, "latitude": 40.75}
WAIT_SECONDS = 1.0


def event(state, kind, **values):
    return {
        "event_id": str(uuid4()), "session_token": state["session_token"],
        "kind": kind, "query": state["query"] or "synthetic", "version": state["version"] or 1,
        **values,
    }


def mock_transport():
    api = Mock()
    api.suggest.return_value = deepcopy(SUGGESTIONS)
    api.retrieve.return_value = deepcopy(SELECTION)
    return api


def pending_call(kind, outcome="success"):
    """Return an unstarted, bounded synthetic request and its explicit gates."""
    state, api = new_address_state(), mock_transport()
    if kind == "retrieve":
        process_address_event(state, event(state, "suggest"), api)
    action = event(state, kind, mapbox_id="synthetic-a")
    entered, release = Event(), Event()
    failures = []

    def transport(*_args):
        entered.set()
        if not release.wait(WAIT_SECONDS):
            raise AssertionError("Synthetic transport release was not signalled")
        if outcome == "error":
            raise MapboxError("Synthetic transport failure")
        if kind == "suggest":
            return [] if outcome == "empty" else deepcopy(SUGGESTIONS)
        return deepcopy(SELECTION)

    getattr(api, kind).side_effect = transport

    def run():
        try:
            process_address_event(state, action, api)
        except BaseException as error:
            failures.append(type(error).__name__)

    worker = Thread(target=run, daemon=True)
    return state, api, action, entered, release, failures, worker


def completion_waiter(state):
    entered, finished = Event(), Event()
    failures = []

    def run():
        entered.set()
        try:
            wait_for_address_request(state)
        except BaseException as error:
            failures.append(type(error).__name__)
        finally:
            finished.set()

    waiter = Thread(target=run, daemon=True)
    return entered, finished, failures, waiter


@pytest.mark.parametrize("kind,outcome,expected", [
    ("suggest", "success", "results"),
    ("suggest", "empty", "empty"),
    ("suggest", "error", "error"),
    ("retrieve", "success", "selected"),
    ("retrieve", "error", "error"),
])
def test_new_render_waits_for_current_request_terminal_state_without_replay(kind, outcome, expected):
    state, api, action, entered, release, failures, worker = pending_call(kind, outcome)
    wait_entered, finished, wait_failures, waiter = completion_waiter(state)
    signalled_states = []
    worker.start()
    try:
        assert entered.wait(WAIT_SECONDS)
        assert state["status"] == ("searching" if kind == "suggest" else "retrieving")
        pending = state["_pending_request"]
        completion = pending["done"]
        original_set = completion.set

        def record_completion():
            signalled_states.append(state["status"])
            original_set()

        completion.set = record_completion
        waiter.start()
        assert wait_entered.wait(WAIT_SECONDS)
        assert not finished.wait(0.02), "A new render must not publish an orphaned pending state"
        release.set()
        assert finished.wait(WAIT_SECONDS)
        worker.join(WAIT_SECONDS)
        waiter.join(WAIT_SECONDS)
        assert not worker.is_alive() and not waiter.is_alive()
        assert not failures and not wait_failures
        assert state["status"] == expected
        assert signalled_states and signalled_states[0] == expected
        assert not state.get("_pending_request")
        before = deepcopy(state["request_counts"])
        wait_for_address_request(state)
        process_address_event(state, action, api)
        assert state["request_counts"] == before
        assert getattr(api, kind).call_count == 1
        if expected == "selected":
            assert state["selected"] == SELECTION
        elif expected == "results":
            assert state["suggestions"] == SUGGESTIONS
        elif expected == "error":
            assert state["selected"] is None and state["error"]
    finally:
        release.set()
        worker.join(WAIT_SECONDS)
        if waiter.ident is not None:
            waiter.join(WAIT_SECONDS)


@pytest.mark.parametrize("first_role", ["pickup", "dropoff"])
def test_one_roles_pending_transport_does_not_block_or_mutate_the_other(first_role):
    state, api, _action, entered, release, failures, worker = pending_call("suggest")
    opposite, opposite_api = new_address_state(), mock_transport()
    tokens = {first_role: state["session_token"], "opposite": opposite["session_token"]}
    assert tokens[first_role] != tokens["opposite"]
    worker.start()
    try:
        assert entered.wait(WAIT_SECONDS)
        process_address_event(opposite, event(opposite, "suggest"), opposite_api)
        wait_for_address_request(opposite)
        assert opposite["status"] == "results" and opposite["suggestions"] == SUGGESTIONS
        assert state["status"] == "searching"
        assert opposite_api.suggest.call_count == 1 and api.suggest.call_count == 1
        release.set()
        worker.join(WAIT_SECONDS)
        assert not worker.is_alive() and not failures
        assert not state.get("_pending_request")
        wait_for_address_request(state)
        assert state["status"] == opposite["status"] == "results"
        assert opposite["session_token"] == tokens["opposite"]
    finally:
        release.set()
        worker.join(WAIT_SECONDS)


@pytest.mark.parametrize("supersession", ["edit", "session"])
def test_superseded_pending_response_cannot_overwrite_current_state(supersession):
    state, api, _action, entered, release, failures, worker = pending_call("suggest")
    waiter = None
    worker.start()
    try:
        assert entered.wait(WAIT_SECONDS)
        if supersession == "edit":
            replacement = mock_transport()
            replacement.suggest.return_value = [{"mapbox_id": "synthetic-b", "label": "Synthetic option B"}]
            process_address_event(state, event(state, "edit", query="synthetic-next", version=2), None)
            process_address_event(state, event(state, "suggest"), replacement)
            expected = {key: deepcopy(state[key]) for key in ("version", "query", "session_token", "status", "suggestions")}
            assert replacement.suggest.call_count == 1
        else:
            state.update(session_token=str(uuid4()), status="idle")
            expected = {key: deepcopy(state[key]) for key in ("version", "query", "session_token", "status", "suggestions")}
        _wait_entered, finished, wait_failures, waiter = completion_waiter(state)
        waiter.start()
        assert finished.wait(0.1), "An irrelevant old request must not delay the current render"
        release.set()
        worker.join(WAIT_SECONDS)
        waiter.join(WAIT_SECONDS)
        assert not worker.is_alive() and not waiter.is_alive()
        assert not failures and not wait_failures
        assert {key: state[key] for key in expected} == expected
        assert api.suggest.call_count == 1
    finally:
        release.set()
        worker.join(WAIT_SECONDS)
        if waiter is not None and waiter.ident is not None:
            waiter.join(WAIT_SECONDS)


@pytest.mark.parametrize("kind", ["suggest", "retrieve"])
def test_expired_current_request_finishes_error_and_ignores_late_response(kind):
    state, api, action, entered, release, failures, worker = pending_call(kind)
    worker.start()
    try:
        assert entered.wait(WAIT_SECONDS)
        retired_token = state["session_token"]
        state["_pending_request"]["deadline"] = monotonic() - 1
        tick = monotonic()
        wait_for_address_request(state)
        assert monotonic() - tick < 0.2, "An expired deadline must not perform a real timeout wait"
        assert state["status"] == "error" and state["error"]
        assert state["session_token"] != retired_token and not state.get("_pending_request")
        release.set()
        worker.join(WAIT_SECONDS)
        assert not worker.is_alive() and not failures
        assert state["status"] == "error" and state["selected"] is None and not state["suggestions"]
        before = deepcopy(state["request_counts"])
        process_address_event(state, action, api)
        wait_for_address_request(state)
        assert state["request_counts"] == before and getattr(api, kind).call_count == 1
    finally:
        release.set()
        worker.join(WAIT_SECONDS)


def test_retrieve_rotation_window_stays_pending_until_selected_is_published(monkeypatch):
    """An own-session rotation is completion in progress, not a newer request."""
    from nyc_taxi.dashboard import address_component as bridge

    state, api, action, entered, release, failures, worker = pending_call("retrieve")
    retired_token = state["session_token"]
    rotation_entered, rotation_release = Event(), Event()
    wait_entered, finished, wait_failures, waiter = completion_waiter(state)
    original_rotate = bridge._rotate

    def pause_after_rotation(target):
        original_rotate(target)
        if target is state and target["status"] == "retrieving":
            rotation_entered.set()
            if not rotation_release.wait(WAIT_SECONDS):
                raise AssertionError("Synthetic rotation release was not signalled")

    monkeypatch.setattr(bridge, "_rotate", pause_after_rotation)
    worker.start()
    try:
        assert entered.wait(WAIT_SECONDS)
        release.set()
        assert rotation_entered.wait(WAIT_SECONDS)
        assert state["session_token"] != retired_token
        assert state["status"] == "retrieving" and state["selected"] is None
        assert not state["_pending_request"]["done"].is_set()
        waiter.start()
        assert wait_entered.wait(WAIT_SECONDS)
        assert not finished.wait(0.02), "Own rotation must not expose retrieving as a finished state"
        rotation_release.set()
        assert finished.wait(WAIT_SECONDS)
        worker.join(WAIT_SECONDS)
        waiter.join(WAIT_SECONDS)
        assert not worker.is_alive() and not waiter.is_alive()
        assert not failures and not wait_failures
        assert state["status"] == "selected" and state["selected"] == SELECTION
        assert not state.get("_pending_request")
        process_address_event(state, action, api)
        wait_for_address_request(state)
        assert api.retrieve.call_count == 1 and api.suggest.call_count == 1
    finally:
        release.set()
        rotation_release.set()
        worker.join(WAIT_SECONDS)
        if waiter.ident is not None:
            waiter.join(WAIT_SECONDS)
