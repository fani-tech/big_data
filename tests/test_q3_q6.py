from __future__ import annotations

from pathlib import Path

from bigdata_project.io import load_trip_2024_csv, load_zones_csv
from bigdata_project.personalization import params_from_am
from bigdata_project.queries.q3 import q3_dataframe, q3_sql
from bigdata_project.queries.q4 import q4_sql
from bigdata_project.queries.q5 import q5_dataframe
from bigdata_project.queries.q6 import q6_dataframe


def _load_2024_and_zones(spark):
    trips = load_trip_2024_csv(spark, path=str(Path("data/sample/yellow_tripdata_2024.csv")))
    zones = load_zones_csv(spark, path=str(Path("data/sample/taxi_zone_lookup.csv")))
    return trips, zones


def test_q3_dataframe(spark):
    params = params_from_am(600)
    trips, zones = _load_2024_and_zones(spark)

    rows = q3_dataframe(trips, zones, params).collect()
    res = {(r["pickup_borough"], r["dropoff_borough"], int(r["trips"])) for r in rows}
    assert res == {("Manhattan", "Queens", 1)}


def test_q3_sql(spark):
    params = params_from_am(600)
    trips, zones = _load_2024_and_zones(spark)

    rows = q3_sql(spark, trips, zones, params).collect()
    res = {(r["pickup_borough"], r["dropoff_borough"], int(r["trips"])) for r in rows}
    assert res == {("Manhattan", "Queens", 1)}


def test_q4_sql(spark):
    params = params_from_am(600)
    trips, zones = _load_2024_and_zones(spark)

    rows = q4_sql(spark, trips, zones, params).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r["pickup_borough"] == "Manhattan"
    assert int(r["Trips"]) == 1
    assert abs(float(r["card_share"]) - 0.0) < 1e-9
    assert r["avg_tip_rate_card"] is None


def test_q5_dataframe(spark):
    params = params_from_am(600)
    trips, zones = _load_2024_and_zones(spark)

    rows = q5_dataframe(trips, zones, params, limit=None).collect()
    res = {(r["borough"], r["zone"], int(r["pickups"]), int(r["dropoffs"]), float(r["imbalance"])) for r in rows}

    assert res == {
        ("Manhattan", "Midtown", 1, 0, 1.0),
        ("Queens", "Jamaica Bay", 0, 1, 1.0),
    }


def test_q6_dataframe(spark):
    params = params_from_am(600)
    trips, zones = _load_2024_and_zones(spark)

    rows = q6_dataframe(trips, zones, params).collect()
    assert len(rows) == 1
    r = rows[0]
    assert int(r["vendor_id"]) == 2
    assert r["pickup_service_zone"] == "Yellow Zone"
    assert int(r["Trips"]) == 1
    assert abs(float(r["TotalRevenue"]) - 9.5) < 1e-9
    assert abs(float(r["CongestionAirport"]) - 0.0) < 1e-9
    assert abs(float(r["Share"]) - 0.0) < 1e-9
    assert abs(float(r["AvgRevenuePerTrip"]) - 9.5) < 1e-9
