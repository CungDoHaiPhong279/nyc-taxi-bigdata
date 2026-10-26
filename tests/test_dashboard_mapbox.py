"""Synthetic Mapbox contracts: no network, tokens, addresses, or stored responses."""

from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import pytest

from nyc_taxi.dashboard import mapbox_client
from nyc_taxi.dashboard.mapbox_client import (
    MAX_RESPONSE_BYTES,
    MapboxClient,
    MapboxError,
    validate_coordinates,
)


TOKEN = "pk.synthetic-only"


def test_search_keeps_supplied_uuid_through_suggest_and_retrieve():
    calls = []
    responses = [
        {"suggestions": [{"mapbox_id": "synthetic/id", "name": "Test A", "place_formatted": "Synthetic region"}]},
        {"features": [{"geometry": {"type": "Point", "coordinates": [-73.9, 40.7]}, "properties": {"name": "Test A"}}]},
    ]

    def transport(url, **options):
        calls.append((urlsplit(url), options))
        return responses.pop(0)

    client = MapboxClient(TOKEN, http=transport)
    session = str(uuid4())
    suggestion = client.suggest("Synthetic A", session)[0]
    assert client.retrieve(suggestion["mapbox_id"], session) == {
        "label": "Test A", "longitude": -73.9, "latitude": 40.7,
    }
    assert suggestion["label"] == "Test A, Synthetic region"
    assert calls[1][0].path.endswith("/synthetic%2Fid")
    for parsed, options in calls:
        assert parsed.scheme == "https" and parsed.netloc == "api.mapbox.com"
        assert parse_qs(parsed.query)["session_token"] == [session]
        assert options == {"timeout": 10, "max_bytes": MAX_RESPONSE_BYTES}
    assert TOKEN not in repr(client)


@pytest.mark.parametrize("query", ["m", " m ", "x" * 250])
def test_autocomplete_accepts_one_character_and_uses_explicit_nyc_search(query):
    calls = []
    session = str(uuid4())

    def transport(url, **options):
        calls.append((parse_qs(urlsplit(url).query), options))
        return {"suggestions": []}

    assert MapboxClient(TOKEN, http=transport).suggest(query, session) == []
    assert len(calls) == 1
    parameters, options = calls[0]
    assert parameters == {
        "q": [query.strip()], "session_token": [session], "country": ["US"],
        "limit": ["5"], "types": ["address,poi"],
        "bbox": ["-74.3,40.45,-73.65,40.95"],
        "proximity": ["-73.9857,40.7484"], "access_token": [TOKEN],
    }
    assert options == {"timeout": 10, "max_bytes": MAX_RESPONSE_BYTES}


@pytest.mark.parametrize("query", [
    "", " ", None, 1, "x" * 251, " " * 250 + "m", "m\n", "m\t", "m\x00", "m\x7f", "m\x85",
])
def test_autocomplete_rejects_empty_control_and_oversized_queries_without_http(query):
    def transport(*args, **kwargs):
        pytest.fail("invalid query must not perform HTTP")

    with pytest.raises(MapboxError):
        MapboxClient(TOKEN, http=transport).suggest(query, str(uuid4()))


def test_suggestion_label_keeps_street_when_full_address_is_missing():
    response = {"suggestions": [{
        "mapbox_id": "synthetic-id", "name": "Synthetic Place",
        "address": "123 Example Street", "place_formatted": "Synthetic region",
    }]}
    assert MapboxClient(TOKEN, http=lambda *args, **kwargs: response).suggest("m", str(uuid4())) == [{
        "mapbox_id": "synthetic-id", "label": "Synthetic Place, 123 Example Street, Synthetic region",
    }]


@pytest.mark.parametrize("response", [
    {}, {"suggestions": None}, {"suggestions": [None]},
    {"suggestions": [{"mapbox_id": "id", "name": "unsafe\nlabel"}]},
    {"suggestions": [{"mapbox_id": "id", "name": "Name", "address": "unsafe\x00street"}]},
    {"suggestions": [{"mapbox_id": "id", "name": "Name"}] * 6},
])
def test_autocomplete_rejects_malformed_or_excess_results(response):
    with pytest.raises(MapboxError):
        MapboxClient(TOKEN, http=lambda *args, **kwargs: response).suggest("m", str(uuid4()))


@pytest.mark.parametrize("session", ["", "not-a-uuid", "00000000-0000-0000-0000-000000000000", 42])
def test_invalid_session_never_calls_transport(session):
    def transport(*args, **kwargs):
        pytest.fail("invalid session must not perform HTTP")
    client = MapboxClient(TOKEN, http=transport)
    with pytest.raises(MapboxError):
        client.suggest("Synthetic A", session)
    with pytest.raises(MapboxError):
        client.retrieve("synthetic-id", session)


@pytest.mark.parametrize("point", [(True, 1), (0, False), (float("nan"), 1), (1, float("inf")), (181, 0), (0, -91), ("1", 2), (10**1000, 1)])
def test_coordinates_reject_invalid_values(point):
    with pytest.raises(MapboxError):
        validate_coordinates(*point)


def test_driving_distance_retains_precision_and_validates_geometry():
    calls = []
    distance = 1609.344123456

    def transport(url, **options):
        calls.append((urlsplit(url), options))
        return {"code": "Ok", "routes": [{"distance": distance, "geometry": {"type": "LineString", "coordinates": [[-73.9, 40.7], [-73.8, 40.8]]}}]}

    result = MapboxClient(TOKEN, http=transport).directions((-73.9, 40.7), (-73.8, 40.8))
    assert result["distance_meters"] == distance
    parsed, options = calls[0]
    assert "/mapbox/driving/" in parsed.path
    assert "traffic" not in parsed.path
    assert parse_qs(parsed.query)["geometries"] == ["geojson"]
    assert options["timeout"] == 20


@pytest.mark.parametrize("response", [
    {"code": "NoRoute", "routes": []},
    {"code": "Ok", "routes": [{"distance": -1}]},
    {"code": "Ok", "routes": [{"distance": float("nan")}]},
    {"code": "Ok", "routes": [{"distance": True}]},
    {"code": "Ok", "routes": [{"distance": 1, "geometry": {"type": "Point", "coordinates": [0, 0]}}]},
    {"code": "Ok", "routes": [{"distance": 1, "geometry": {"type": "LineString", "coordinates": [[0, 0], [999, 1]]}}]},
])
def test_bad_routes_are_rejected(response):
    with pytest.raises(MapboxError):
        MapboxClient(TOKEN, http=lambda *args, **kwargs: response).directions((0, 0), (1, 1))


@pytest.mark.parametrize("response", [
    {"features": []},
    {"features": [{"geometry": {"type": "Point", "coordinates": [1]}, "properties": {"name": "Test"}}]},
    {"features": [{"geometry": {"type": "Point", "coordinates": [999, 0]}, "properties": {"name": "Test"}}]},
])
def test_malformed_retrieve_is_rejected(response):
    with pytest.raises(MapboxError):
        MapboxClient(TOKEN, http=lambda *args, **kwargs: response).retrieve("id", str(uuid4()))


@pytest.mark.parametrize("failure", [
    HTTPError("https://invalid/?access_token=" + TOKEN, 401, "private address", {}, None),
    HTTPError("https://invalid/?access_token=" + TOKEN, 403, "private address", {}, None),
    HTTPError("https://invalid/?access_token=" + TOKEN, 429, "private address", {}, None),
    HTTPError("https://invalid/?access_token=" + TOKEN, 503, "private address", {}, None),
    TimeoutError("private address " + TOKEN),
    URLError("private address " + TOKEN),
    ValueError("private address " + TOKEN),
])
def test_api_failures_are_safe_and_never_retried(failure):
    calls = []

    def transport(url, **options):
        calls.append(url)
        raise failure

    with pytest.raises(MapboxError) as caught:
        MapboxClient(TOKEN, http=transport).directions((0, 0), (1, 1))
    assert len(calls) == 1
    assert TOKEN not in str(caught.value)
    assert "private address" not in str(caught.value)
    assert "https://" not in str(caught.value)
    assert caught.value.__suppress_context__


@pytest.mark.parametrize(
    "body", [b"not-json", b"[]", b'{"value": NaN}', b"x" * (MAX_RESPONSE_BYTES + 1)],
    ids=["invalid-json", "wrong-top-level", "nonfinite-json", "oversized-body"],
)
def test_default_transport_bounds_and_rejects_bad_json_offline(monkeypatch, body):
    observed = []

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self, count):
            observed.append(count)
            return body[:count]

    class Opener:
        def open(self, request, *, timeout):
            assert request.full_url.startswith("https://api.mapbox.com/")
            assert timeout == 10
            return Response()

    def opener_factory(handler):
        assert isinstance(handler, mapbox_client._NoRedirect)
        return Opener()

    monkeypatch.setattr(mapbox_client, "build_opener", opener_factory)
    with pytest.raises(MapboxError):
        MapboxClient(TOKEN).suggest("Synthetic A", str(uuid4()))
    assert observed == [MAX_RESPONSE_BYTES + 1]


def test_default_transport_does_not_follow_redirects():
    handler = mapbox_client._NoRedirect()
    assert handler.redirect_request(None, None, 302, "Moved", {}, "https://elsewhere.invalid/") is None


@pytest.mark.parametrize("token", ["", None, "pk.has space", "pk.line\nbreak"])
def test_invalid_token_is_rejected_without_exposing_value(token):
    with pytest.raises(MapboxError) as caught:
        MapboxClient(token)
    assert "pk." not in str(caught.value)
