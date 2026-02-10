from __future__ import annotations

import csv
import heapq
from datetime import datetime
from typing import Iterable, Iterator, Tuple

from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from bigdata_project.personalization import PersonalizationParams
from bigdata_project.schemas import Q2_RESULT_SCHEMA


def q2_dataframe(trips_2015: DataFrame, params: PersonalizationParams) -> DataFrame:
    hours = sorted(params.hours_set)
    k = int(params.k)

    df = trips_2015.select(
        "vendor_id",
        F.date_format(F.col("pickup_datetime"), "yyyy-MM-dd").alias("date"),
        F.hour("pickup_datetime").alias("hour_of_day"),
        "trip_distance",
        "fare_amount",
        "tip_amount",
    ).where(
        F.col("hour_of_day").isin(hours)
        & (F.col("trip_distance") > 0)
        & (F.col("fare_amount") > 0)
        & F.col("vendor_id").isNotNull()
        & F.col("date").isNotNull()
    )

    df = df.withColumn("tip_per_mile", (F.col("tip_amount") / F.col("trip_distance")).cast("double"))

    agg = df.groupBy("vendor_id", "date").agg(F.avg("tip_per_mile").alias("avg_tip_per_mile"))

    w = Window.partitionBy("vendor_id").orderBy(F.col("avg_tip_per_mile").desc_nulls_last())

    out = (
        agg.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") <= F.lit(k))
        .select("vendor_id", "date", "avg_tip_per_mile")
        .orderBy("vendor_id", F.col("avg_tip_per_mile").desc_nulls_last())
    )

    return out


def q2_sql(spark: SparkSession, trips_2015: DataFrame, params: PersonalizationParams) -> DataFrame:
    trips_2015.createOrReplaceTempView("trips2015")

    hours = sorted(int(x) for x in params.hours_set)
    hours_sql = ", ".join(str(x) for x in hours)
    k = int(params.k)

    query = f"""
    WITH base AS (
      SELECT
        vendor_id,
        date_format(pickup_datetime, 'yyyy-MM-dd') AS date,
        (tip_amount / trip_distance) AS tip_per_mile
      FROM trips2015
      WHERE
        hour(pickup_datetime) IN ({hours_sql})
        AND trip_distance > 0
        AND fare_amount > 0
    ), agg AS (
      SELECT
        vendor_id,
        date,
        AVG(tip_per_mile) AS avg_tip_per_mile
      FROM base
      GROUP BY vendor_id, date
    ), ranked AS (
      SELECT
        vendor_id,
        date,
        avg_tip_per_mile,
        ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY avg_tip_per_mile DESC NULLS LAST) AS rn
      FROM agg
    )
    SELECT vendor_id, date, avg_tip_per_mile
    FROM ranked
    WHERE rn <= {k}
    ORDER BY vendor_id, avg_tip_per_mile DESC NULLS LAST
    """

    return spark.sql(query)


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


def q2_rdd(spark: SparkSession, *, csv_path: str, params: PersonalizationParams) -> DataFrame:
    sc: SparkContext = spark.sparkContext
    rdd = sc.textFile(csv_path)

    header_line = rdd.first()
    header = next(csv.reader([header_line]))

    def idx(name: str) -> int:
        return header.index(name)

    i_vendor = idx("VendorID")
    i_pickup_dt = idx("tpep_pickup_datetime")
    i_trip_distance = idx("trip_distance")
    i_fare_amount = idx("fare_amount")
    i_tip_amount = idx("tip_amount")

    b_idx = sc.broadcast(
        {
            "vendor": i_vendor,
            "pdt": i_pickup_dt,
            "dist": i_trip_distance,
            "fare": i_fare_amount,
            "tip": i_tip_amount,
        }
    )
    b_hours = sc.broadcast(set(params.hours_set))
    b_k = sc.broadcast(int(params.k))

    def drop_header(partition_index: int, iterator: Iterable[str]) -> Iterator[str]:
        it = iter(iterator)
        if partition_index == 0:
            next(it, None)
        return it

    data = rdd.mapPartitionsWithIndex(drop_header)

    def map_partition(lines: Iterable[str]) -> Iterator[Tuple[Tuple[int, str], Tuple[float, int]]]:
        acc: dict[Tuple[int, str], Tuple[float, int]] = {}
        for row in csv.reader(lines):
            if len(row) <= max(b_idx.value.values()):
                continue

            try:
                vendor = int(row[b_idx.value["vendor"]])
            except ValueError:
                continue

            try:
                pdt = _parse_dt(row[b_idx.value["pdt"]])
            except Exception:
                continue

            hour = int(pdt.hour)
            if hour not in b_hours.value:
                continue

            try:
                trip_distance = float(row[b_idx.value["dist"]])
                fare_amount = float(row[b_idx.value["fare"]])
                tip_amount = float(row[b_idx.value["tip"]])
            except ValueError:
                continue

            if trip_distance <= 0.0 or fare_amount <= 0.0:
                continue

            tip_per_mile = tip_amount / trip_distance
            date_str = pdt.strftime("%Y-%m-%d")

            key = (vendor, date_str)
            s, c = acc.get(key, (0.0, 0))
            acc[key] = (s + float(tip_per_mile), c + 1)

        for k, v in acc.items():
            yield k, v

    def merge_sumcnt(a: Tuple[float, int], b: Tuple[float, int]) -> Tuple[float, int]:
        return a[0] + b[0], a[1] + b[1]

    sums = data.mapPartitions(map_partition).reduceByKey(merge_sumcnt)

    avgs = sums.map(lambda kv: (kv[0][0], (float(kv[1][0]) / float(kv[1][1]) if kv[1][1] else None, kv[0][1])))

    def seq(heap: list[Tuple[float, str]], item: Tuple[float | None, str]) -> list[Tuple[float, str]]:
        avg, date_str = item
        if avg is None:
            return heap
        k = int(b_k.value)
        if len(heap) < k:
            heapq.heappush(heap, (float(avg), date_str))
        else:
            if float(avg) > heap[0][0]:
                heapq.heapreplace(heap, (float(avg), date_str))
        return heap

    def comb(h1: list[Tuple[float, str]], h2: list[Tuple[float, str]]) -> list[Tuple[float, str]]:
        for it in h2:
            seq(h1, it)
        return h1

    topk = avgs.aggregateByKey([], seq, comb)

    flat = topk.flatMap(
        lambda kv: [(int(kv[0]), d, float(a)) for (a, d) in sorted(kv[1], key=lambda x: x[0], reverse=True)]
    )

    df_out = spark.createDataFrame(flat, schema=Q2_RESULT_SCHEMA)
    return df_out.orderBy("vendor_id", F.col("avg_tip_per_mile").desc_nulls_last())
