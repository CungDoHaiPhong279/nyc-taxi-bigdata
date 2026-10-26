"""Submitted-route bridge contracts; no network, Streamlit, or model required."""

from copy import deepcopy

import pytest

from nyc_taxi.dashboard.route_map import route_map_arguments


GEOMETRY = {"type": "LineString", "coordinates": [[-73.98, 40.75], [-73.97, 40.76]]}


def arguments(**changes):
    values = dict(geometry=deepcopy(GEOMETRY), submission_id="synthetic-submission",
                  pickup=[-73.98123456789, 40.751], dropoff=[-73.969, 40.761],
                  access_token="pk.synthetic_test_value", online=True)
    values.update(changes)
    return route_map_arguments(**values)


def test_map_bridge_preserves_route_and_submission_without_rounding():
    geometry = deepcopy(GEOMETRY)
    geometry["coordinates"][0][0] = -73.980123456789
    actual = arguments(geometry=geometry)
    assert actual["geometry"] == geometry
    assert actual["submission_id"] == "synthetic-submission"
    geometry["coordinates"][0][0] = 0
    assert actual["geometry"]["coordinates"][0][0] == -73.980123456789


def test_markers_use_submitted_points_not_snapped_route_ends_or_address_html():
    pickup, dropoff = [-73.98123456789, 40.751], [-73.969, 40.761]
    actual = arguments(pickup=pickup, dropoff=dropoff)
    assert actual["pickup"] == pickup != actual["geometry"]["coordinates"][0]
    assert actual["dropoff"] == dropoff != actual["geometry"]["coordinates"][-1]
    pickup[0] = dropoff[0] = 0
    assert actual["pickup"][0] == -73.98123456789
    assert actual["dropoff"][0] == -73.969
    assert set(actual) == {"online", "submission_id", "access_token", "geometry", "pickup", "dropoff"}


@pytest.mark.parametrize("slot", ["pickup", "dropoff"])
@pytest.mark.parametrize("point", [None, {}, "<img onerror=alert(1)>", [0], [0, 0, 0],
                                    [True, 0], [float("nan"), 0], [0, float("inf")],
                                    [181, 0], [0, -91], [10 ** 400, 0]])
def test_invalid_marker_never_exposes_token_or_route(slot, point):
    assert arguments(**{slot: point}) == {"online": False}


@pytest.mark.parametrize("changes", [
    {"online": False}, {"online": "true"}, {"access_token": ""},
    {"access_token": "sk.synthetic_secret"}, {"access_token": "pk.bad\nvalue"},
    {"submission_id": ""}, {"submission_id": "a" * 129},
    {"submission_id": None},
    {"geometry": None}, {"geometry": {"type": "Polygon", "coordinates": []}},
    {"geometry": {"type": "LineString", "coordinates": [[0, 0]]}},
    {"geometry": {"type": "LineString", "coordinates": [[float("nan"), 1], [0, 0]]}},
    {"geometry": {"type": "LineString", "coordinates": [[0, float("inf")], [0, 0]]}},
    {"geometry": {"type": "LineString", "coordinates": [[181, 0], [0, 0]]}},
    {"geometry": {"type": "LineString", "coordinates": [[0, 91], [0, 0]]}},
    {"geometry": {"type": "LineString", "coordinates": [[True, 0], [0, 0]]}},
    {"geometry": {"type": "LineString", "coordinates": [[10 ** 400, 0], [0, 0]]}},
    {"geometry": {"type": "LineString", "coordinates": [[0, 0, 0], [0, 0]]}},
])
def test_invalid_or_disabled_bridge_never_sends_token_or_coordinates(changes):
    assert arguments(**changes) == {"online": False}
