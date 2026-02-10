from __future__ import annotations

from pathlib import Path

from bigdata_project.io import load_trip_2015_csv
from bigdata_project.personalization import params_from_am
from bigdata_project.queries.q2 import q2_dataframe, q2_rdd, q2_sql


def test_q2_dataframe(spark):
    params = params_from_am(600)
    path = str(Path("data/sample/yellow_tripdata_2015.csv"))
    trips = load_trip_2015_csv(spark, path=path)

    rows = q2_dataframe(trips, params).collect()
    res = {(int(r["vendor_id"]), r["date"], float(r["avg_tip_per_mile"])) for r in rows}

    expected = {
        (1, "2015-01-01", 0.75),
        (2, "2015-01-01", 0.0),
    }

    assert res == expected


def test_q2_sql(spark):
    params = params_from_am(600)
    path = str(Path("data/sample/yellow_tripdata_2015.csv"))
    trips = load_trip_2015_csv(spark, path=path)

    rows = q2_sql(spark, trips, params).collect()
    res = {(int(r["vendor_id"]), r["date"], float(r["avg_tip_per_mile"])) for r in rows}

    expected = {
        (1, "2015-01-01", 0.75),
        (2, "2015-01-01", 0.0),
    }

    assert res == expected


def test_q2_rdd(spark):
    params = params_from_am(600)
    csv_path = str(Path("data/sample/yellow_tripdata_2015.csv"))

    rows = q2_rdd(spark, csv_path=csv_path, params=params).collect()
    res = {(int(r["vendor_id"]), r["date"], float(r["avg_tip_per_mile"])) for r in rows}

    expected = {
        (1, "2015-01-01", 0.75),
        (2, "2015-01-01", 0.0),
    }

    assert res == expected
