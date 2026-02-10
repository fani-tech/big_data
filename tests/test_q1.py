from __future__ import annotations

import math
from pathlib import Path

from bigdata_project.io import load_trip_2015_csv
from bigdata_project.personalization import params_from_am
from bigdata_project.queries.q1 import q1_dataframe, q1_dataframe_udf, q1_rdd


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    lat1r = math.radians(lat1)
    lon1r = math.radians(lon1)
    lat2r = math.radians(lat2)
    lon2r = math.radians(lon2)
    dlat = lat2r - lat1r
    dlon = lon2r - lon1r
    a = math.sin(dlat / 2.0) ** 2 + math.cos(lat1r) * math.cos(lat2r) * (math.sin(dlon / 2.0) ** 2)
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return r * c


def test_q1_dataframe(spark):
    params = params_from_am(600)
    path = str(Path("data/sample/yellow_tripdata_2015.csv"))
    trips = load_trip_2015_csv(spark, path=path)

    rows = q1_dataframe(trips, params).collect()
    res = {int(r["hour_of_day"]): (int(r["Trips"]), float(r["AvgDurationMin"]), float(r["P90HaversineKm"])) for r in rows}

    dist_h0 = _haversine_km(40.0, -74.0, 40.0, -73.0)
    dist_h1 = _haversine_km(40.0, -73.0, 41.0, -73.0)

    assert set(res.keys()) == {0, 1}
    assert res[0][0] == 2
    assert abs(res[0][1] - 10.0) < 1e-9
    assert abs(res[0][2] - dist_h0) < 1.0

    assert res[1][0] == 1
    assert abs(res[1][1] - 20.0) < 1e-9
    assert abs(res[1][2] - dist_h1) < 1.0


def test_q1_dataframe_udf(spark):
    params = params_from_am(600)
    path = str(Path("data/sample/yellow_tripdata_2015.csv"))
    trips = load_trip_2015_csv(spark, path=path)

    rows = q1_dataframe_udf(trips, params).collect()
    res = {int(r["hour_of_day"]): (int(r["Trips"]), float(r["AvgDurationMin"]), float(r["P90HaversineKm"])) for r in rows}

    dist_h0 = _haversine_km(40.0, -74.0, 40.0, -73.0)
    dist_h1 = _haversine_km(40.0, -73.0, 41.0, -73.0)

    assert set(res.keys()) == {0, 1}
    assert res[0][0] == 2
    assert abs(res[0][1] - 10.0) < 1e-9
    assert abs(res[0][2] - dist_h0) < 1.0

    assert res[1][0] == 1
    assert abs(res[1][1] - 20.0) < 1e-9
    assert abs(res[1][2] - dist_h1) < 1.0


def test_q1_rdd(spark):
    params = params_from_am(600)
    csv_path = str(Path("data/sample/yellow_tripdata_2015.csv"))

    df = q1_rdd(spark, csv_path=csv_path, params=params)

    rows = df.collect()
    res = {int(r["hour_of_day"]): (int(r["Trips"]), float(r["AvgDurationMin"]), float(r["P90HaversineKm"])) for r in rows}

    dist_h0 = _haversine_km(40.0, -74.0, 40.0, -73.0)
    dist_h1 = _haversine_km(40.0, -73.0, 41.0, -73.0)

    assert set(res.keys()) == {0, 1}
    assert res[0][0] == 2
    assert abs(res[0][1] - 10.0) < 1e-9
    assert abs(res[0][2] - dist_h0) < 1.5

    assert res[1][0] == 1
    assert abs(res[1][1] - 20.0) < 1e-9
    assert abs(res[1][2] - dist_h1) < 1.5
