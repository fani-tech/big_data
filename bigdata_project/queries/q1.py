from __future__ import annotations

import csv
import math
from datetime import datetime
from typing import Iterable, Iterator, Tuple

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from bigdata_project.io import haversine_km
from bigdata_project.personalization import PersonalizationParams
from bigdata_project.schemas import Q1_RESULT_SCHEMA


def _coords_ok(lat: F.Column, lon: F.Column) -> F.Column:
    return (
        lat.isNotNull()
        & lon.isNotNull()
        & (lat != 0)
        & (lon != 0)
        & lat.between(-90.0, 90.0)
        & lon.between(-180.0, 180.0)
    )


def q1_dataframe(trips_2015: DataFrame, params: PersonalizationParams) -> DataFrame:
    hours = sorted(params.hours_set)

    df = trips_2015.select(
        F.hour("pickup_datetime").alias("hour_of_day"),
        (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")).cast("long").alias("duration_seconds"),
        F.col("pickup_latitude").alias("pickup_latitude"),
        F.col("pickup_longitude").alias("pickup_longitude"),
        F.col("dropoff_latitude").alias("dropoff_latitude"),
        F.col("dropoff_longitude").alias("dropoff_longitude"),
    ).where(
        F.col("hour_of_day").isin(hours)
        & (F.col("duration_seconds") > 0)
        & _coords_ok(F.col("pickup_latitude"), F.col("pickup_longitude"))
        & _coords_ok(F.col("dropoff_latitude"), F.col("dropoff_longitude"))
    )

    df = (
        df.withColumn("duration_minutes", (F.col("duration_seconds") / F.lit(60.0)).cast("double"))
        .withColumn(
            "haversine_km",
            haversine_km(
                F.col("pickup_latitude"),
                F.col("pickup_longitude"),
                F.col("dropoff_latitude"),
                F.col("dropoff_longitude"),
            ),
        )
    )

    out = (
        df.groupBy("hour_of_day")
        .agg(
            F.count(F.lit(1)).alias("Trips"),
            F.avg("duration_minutes").alias("AvgDurationMin"),
            F.expr("percentile_approx(haversine_km, 0.9)").alias("P90HaversineKm"),
        )
        .orderBy("hour_of_day")
        .withColumn("hour_str", F.format_string("%02d", F.col("hour_of_day")))
    )

    return out


def q1_dataframe_udf(trips_2015: DataFrame, params: PersonalizationParams) -> DataFrame:
    hours = sorted(params.hours_set)

    @F.udf(returnType=T.DoubleType())
    def haversine_udf(lat1: float | None, lon1: float | None, lat2: float | None, lon2: float | None) -> float | None:
        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            return None
        r = 6371.0
        lat1r = math.radians(float(lat1))
        lon1r = math.radians(float(lon1))
        lat2r = math.radians(float(lat2))
        lon2r = math.radians(float(lon2))
        dlat = lat2r - lat1r
        dlon = lon2r - lon1r
        a = math.sin(dlat / 2.0) ** 2 + math.cos(lat1r) * math.cos(lat2r) * (math.sin(dlon / 2.0) ** 2)
        c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
        return r * c

    df = trips_2015.select(
        F.hour("pickup_datetime").alias("hour_of_day"),
        (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")).cast("long").alias("duration_seconds"),
        F.col("pickup_latitude").alias("pickup_latitude"),
        F.col("pickup_longitude").alias("pickup_longitude"),
        F.col("dropoff_latitude").alias("dropoff_latitude"),
        F.col("dropoff_longitude").alias("dropoff_longitude"),
    ).where(
        F.col("hour_of_day").isin(hours)
        & (F.col("duration_seconds") > 0)
        & _coords_ok(F.col("pickup_latitude"), F.col("pickup_longitude"))
        & _coords_ok(F.col("dropoff_latitude"), F.col("dropoff_longitude"))
    )

    df = df.withColumn("duration_minutes", (F.col("duration_seconds") / F.lit(60.0)).cast("double")).withColumn(
        "haversine_km",
        haversine_udf(
            F.col("pickup_latitude"),
            F.col("pickup_longitude"),
            F.col("dropoff_latitude"),
            F.col("dropoff_longitude"),
        ),
    )

    out = (
        df.groupBy("hour_of_day")
        .agg(
            F.count(F.lit(1)).alias("Trips"),
            F.avg("duration_minutes").alias("AvgDurationMin"),
            F.expr("percentile_approx(haversine_km, 0.9)").alias("P90HaversineKm"),
        )
        .orderBy("hour_of_day")
        .withColumn("hour_str", F.format_string("%02d", F.col("hour_of_day")))
    )

    return out


def _parse_dt(value: str) -> datetime:
    v = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    v2 = v.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(v2, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unparseable datetime: {value}")


def _haversine_km_py(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
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


def q1_rdd(spark: SparkSession, *, csv_path: str, params: PersonalizationParams) -> DataFrame:
    sc: SparkContext = spark.sparkContext
    rdd = sc.textFile(csv_path)

    header_line = rdd.first()
    header = next(csv.reader([header_line]))

    def idx(name: str) -> int:
        return header.index(name)

    i_pickup_dt = idx("tpep_pickup_datetime")
    i_dropoff_dt = idx("tpep_dropoff_datetime")
    i_pickup_lon = idx("pickup_longitude")
    i_pickup_lat = idx("pickup_latitude")
    i_dropoff_lon = idx("dropoff_longitude")
    i_dropoff_lat = idx("dropoff_latitude")

    b_idx = sc.broadcast(
        {
            "pdt": i_pickup_dt,
            "ddt": i_dropoff_dt,
            "plon": i_pickup_lon,
            "plat": i_pickup_lat,
            "dlon": i_dropoff_lon,
            "dlat": i_dropoff_lat,
        }
    )
    b_hours = sc.broadcast(set(params.hours_set))

    bin_width = 0.5
    max_km = 500.0
    num_bins = int(max_km / bin_width)
    bins_len = num_bins + 1

    def drop_header(partition_index: int, iterator: Iterable[str]) -> Iterator[str]:
        it = iter(iterator)
        if partition_index == 0:
            next(it, None)
        return it

    data = rdd.mapPartitionsWithIndex(drop_header)

    def map_partition(lines: Iterable[str]) -> Iterator[Tuple[int, Tuple[int, float, list[int]]]]:
        acc: dict[int, Tuple[int, float, list[int]]] = {}
        for row in csv.reader(lines):
            if len(row) <= max(b_idx.value.values()):
                continue

            try:
                plat = float(row[b_idx.value["plat"]])
                plon = float(row[b_idx.value["plon"]])
                dlat = float(row[b_idx.value["dlat"]])
                dlon = float(row[b_idx.value["dlon"]])
            except ValueError:
                continue

            if (
                plat == 0.0
                or plon == 0.0
                or dlat == 0.0
                or dlon == 0.0
                or plat < -90.0
                or plat > 90.0
                or dlat < -90.0
                or dlat > 90.0
                or plon < -180.0
                or plon > 180.0
                or dlon < -180.0
                or dlon > 180.0
            ):
                continue

            try:
                pdt = _parse_dt(row[b_idx.value["pdt"]])
                ddt = _parse_dt(row[b_idx.value["ddt"]])
            except Exception:
                continue

            hour = int(pdt.hour)
            if hour not in b_hours.value:
                continue

            duration_s = int((ddt - pdt).total_seconds())
            if duration_s <= 0:
                continue

            dist_km = _haversine_km_py(plat, plon, dlat, dlon)
            if dist_km < 0.0 or math.isnan(dist_km) or math.isinf(dist_km):
                continue

            bin_idx = int(dist_km / bin_width)
            if bin_idx >= num_bins:
                bin_idx = num_bins

            if hour not in acc:
                acc[hour] = (0, 0.0, [0] * bins_len)

            c, dur_sum, hist = acc[hour]
            hist[bin_idx] += 1
            acc[hour] = (c + 1, dur_sum + (duration_s / 60.0), hist)

        for h, v in acc.items():
            yield h, v

    def merge(a: Tuple[int, float, list[int]], b: Tuple[int, float, list[int]]) -> Tuple[int, float, list[int]]:
        c1, d1, h1 = a
        c2, d2, h2 = b
        return c1 + c2, d1 + d2, [h1[i] + h2[i] for i in range(bins_len)]

    def p90_from_hist(hist: list[int], total: int) -> float | None:
        if total <= 0:
            return None
        target = int(math.ceil(0.9 * total))
        if target <= 0:
            return None
        cum = 0
        for i, n in enumerate(hist):
            cum += int(n)
            if cum >= target:
                if i >= num_bins:
                    return float(max_km)
                return float(i * bin_width)
        return float(max_km)

    agg = data.mapPartitions(map_partition).reduceByKey(merge).sortByKey(ascending=True)

    rows = agg.map(
        lambda kv: (
            int(kv[0]),
            int(kv[1][0]),
            float(kv[1][1]) / float(kv[1][0]) if kv[1][0] else None,
            p90_from_hist(kv[1][2], int(kv[1][0])),
        )
    )

    df_out = spark.createDataFrame(rows, schema=Q1_RESULT_SCHEMA)
    return df_out.withColumn("hour_str", F.format_string("%02d", F.col("hour_of_day")))
