import h3
import numpy as np
import pytest

from common.config import time_slot
from common.geo import (describe_location, haversine_km, latlng_to_cell, nearest_place, region_case_sql,
                        region_of)


def test_haversine_known_distance():
    # Times Square -> Shibuya crossing is ~10,850 km.
    d = haversine_km(40.7580, -73.9855, 35.6595, 139.7005)
    assert 10_800 < d < 10_900


def test_haversine_vectorised_and_zero():
    d = haversine_km(40.0, -74.0, np.array([40.0, 41.0]), np.array([-74.0, -74.0]))
    assert d[0] == pytest.approx(0.0, abs=1e-9)
    assert d[1] == pytest.approx(111.2, abs=0.5)        # one degree of latitude


def test_h3_cell_contains_point():
    cell = latlng_to_cell(40.7505, -73.9934, 8)
    assert h3.get_resolution(cell) == 8
    assert h3.latlng_to_cell(*h3.cell_to_latlng(cell), 8) == cell
    assert latlng_to_cell(float("nan"), 0.0, 8) is None


def test_nearest_place_labels():
    assert nearest_place("nyc", 40.7579, -73.9857) == "Times Square"
    assert nearest_place("tky", 35.6581, 139.7017) == "Shibuya"
    assert nearest_place("nyc", 35.0, 139.0) is None                 # far from every NYC landmark
    assert describe_location("tky", 35.6909, 139.7003) == "near Shinjuku"
    assert describe_location("gowalla", 1.0, 2.0) == "1.000, 2.000"


@pytest.mark.parametrize("lat,lon,region", [
    (30.27, -97.74, "North America"), (59.33, 18.07, "Europe"), (35.68, 139.69, "Asia"),
    (-33.87, 151.21, "Oceania"), (-23.55, -46.63, "South America"), (-1.29, 36.82, "Africa"),
    (-75.0, 0.0, "Other"),
])
def test_region_of(lat, lon, region):
    assert region_of(lat, lon) == region


def test_region_case_sql_mirrors_python():
    sql = region_case_sql("a", "b")
    assert sql.startswith("CASE WHEN a BETWEEN") and sql.endswith("ELSE 'Other' END")
    assert sql.count("WHEN") == 6


def test_time_slots_cover_the_day():
    assert [time_slot(h) for h in (0, 5, 6, 11, 12, 17, 18, 23)] == [
        "night", "night", "morning", "morning", "afternoon", "afternoon", "evening", "evening"]
