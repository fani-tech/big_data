from __future__ import annotations

from pathlib import Path

from bigdata_project.io import (
    load_trip_2015_csv,
    load_trip_2015_parquet,
    load_trip_2024_csv,
    load_trip_2024_parquet,
    load_zones_csv,
    load_zones_parquet,
)
from bigdata_project.personalization import params_from_am
from bigdata_project.queries.q1 import q1_dataframe
from bigdata_project.queries.q3 import q3_dataframe


def test_trip_2015_parquet_roundtrip_cleaner(spark, tmp_path: Path):
    params = params_from_am(600)
    csv_path = str(Path("data/sample/yellow_tripdata_2015.csv"))

    trips_csv = load_trip_2015_csv(spark, path=csv_path)

    out_dir = tmp_path / "trip_2015"
    trips_csv.write.mode("overwrite").parquet(str(out_dir))

    trips_parquet = load_trip_2015_parquet(spark, path=str(out_dir))

    res_csv = {(int(r["hour_of_day"]), int(r["Trips"]), round(float(r["AvgDurationMin"]), 6), round(float(r["P90HaversineKm"]), 1)) for r in q1_dataframe(trips_csv, params).collect()}
    res_parquet = {(int(r["hour_of_day"]), int(r["Trips"]), round(float(r["AvgDurationMin"]), 6), round(float(r["P90HaversineKm"]), 1)) for r in q1_dataframe(trips_parquet, params).collect()}

    assert res_csv == res_parquet


def test_trip_2024_and_zones_parquet_roundtrip_cleaner(spark, tmp_path: Path):
    params = params_from_am(600)
    trip_csv_path = str(Path("data/sample/yellow_tripdata_2024.csv"))
    zones_csv_path = str(Path("data/sample/taxi_zone_lookup.csv"))

    trips_csv = load_trip_2024_csv(spark, path=trip_csv_path)
    zones_csv = load_zones_csv(spark, path=zones_csv_path)
    assert "pickup_date" in trips_csv.columns

    trips_dir = tmp_path / "trip_2024"
    zones_dir = tmp_path / "zones"

    trips_csv.write.mode("overwrite").partitionBy("pickup_date").parquet(str(trips_dir))
    zones_csv.write.mode("overwrite").parquet(str(zones_dir))

    trips_parquet = load_trip_2024_parquet(spark, path=str(trips_dir))
    zones_parquet = load_zones_parquet(spark, path=str(zones_dir))
    assert "pickup_date" in trips_parquet.columns

    res_csv = {(r["pickup_borough"], r["dropoff_borough"], int(r["trips"])) for r in q3_dataframe(trips_csv, zones_csv, params).collect()}
    res_parquet = {(r["pickup_borough"], r["dropoff_borough"], int(r["trips"])) for r in q3_dataframe(trips_parquet, zones_parquet, params).collect()}

    assert res_csv == res_parquet
