"""Small, network-free Python bridge for the submitted-route map component."""

from __future__ import annotations

from copy import deepcopy
from math import isfinite
from pathlib import Path
import re


_PUBLIC_TOKEN = re.compile(r"pk\.[A-Za-z0-9._-]+\Z")


def _valid_point(point: object) -> bool:
    if not isinstance(point, (list, tuple)) or len(point) != 2:
        return False
    try:
        return (all(not isinstance(value, bool) and isinstance(value, (int, float))
                    and isfinite(value) for value in point)
                and -180 <= point[0] <= 180 and -90 <= point[1] <= 90)
    except (OverflowError, ValueError, TypeError):
        return False


def route_map_arguments(geometry: object, *, pickup: object, dropoff: object,
                        submission_id: str | None,
                        access_token: str, online: bool) -> dict:
    """Fail closed before sending a runtime token or coordinates to a browser.

    The route comes from the completed submission, never from draft form values.
    No request, storage, model operation, or Directions calculation occurs here.
    """
    disabled = {"online": False}
    if online is not True:
        return disabled
    if (not isinstance(access_token, str)
            or not _PUBLIC_TOKEN.fullmatch(access_token)
            or not isinstance(submission_id, str)
            or not 1 <= len(submission_id) <= 128):
        return disabled
    if not isinstance(geometry, dict) or geometry.get("type") != "LineString":
        return disabled
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list) or not 2 <= len(coordinates) <= 20_000:
        return disabled
    if not all(_valid_point(point) for point in [*coordinates, pickup, dropoff]):
        return disabled
    return {"online": True, "submission_id": submission_id,
            "access_token": access_token,
            "geometry": {"type": "LineString", "coordinates": deepcopy(coordinates)},
            "pickup": list(pickup), "dropoff": list(dropoff)}


def route_map(geometry: object, *, pickup: object, dropoff: object,
              submission_id: str | None, access_token: str,
              online: bool, key: str) -> None:
    """Render one stable component; a rerun does not recreate its map instance."""
    import streamlit.components.v1 as components

    component = components.declare_component(
        "nyc_submitted_route_map",
        path=str(Path(__file__).with_name("route_map_frontend")),
    )
    component(**route_map_arguments(
        geometry, pickup=pickup, dropoff=dropoff,
        submission_id=submission_id, access_token=access_token,
        online=online,
    ), default=None, key=key)
