from __future__ import annotations

from typing import Iterable, Optional

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def _first_existing_col(df: DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None


def _first_existing_col_case_insensitive(df: DataFrame, candidates: Iterable[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for cand in candidates:
        found = lower_map.get(cand.lower())
        if found:
            return found
    return None


def _require_any_of_case_insensitive(df: DataFrame, *, field: str, candidates: Iterable[str]) -> str:
    found = _first_existing_col_case_insensitive(df, candidates)
    if found:
        return found

    raise ValueError(
        f"Λείπει απαιτούμενη στήλη για '{field}'. Αναμενόμενα (ένα από): {list(candidates)}. "
        "Ελέγξτε ότι χρησιμοποιείτε το σωστό dataset από το HDFS του εργαστηρίου."
    )


def _ensure_required_columns(df: DataFrame, *, required: dict[str, list[str]]) -> None:
    missing: list[str] = []
    for logical, candidates in required.items():
        if _first_existing_col_case_insensitive(df, candidates) is None:
            missing.append(f"{logical} ({candidates})")

    if missing:
        raise ValueError(
            "Το dataset δεν περιέχει όλα τα απαιτούμενα πεδία. Λείπουν: "
            + "; ".join(missing)
            + ". "
            "Αυτό συνήθως σημαίνει ότι φορτώθηκε λάθος αρχείο (π.χ. parquet χωρίς GPS αντί για το HDFS 2015 CSV του εργαστηρίου)."
        )


def _parse_timestamp_multi(col: Column) -> Column:
    cleaned = F.trim(col)
    cleaned2 = F.regexp_replace(cleaned, "T", " ")

    return F.coalesce(
        F.to_timestamp(cleaned, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(cleaned, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(cleaned2, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(cleaned2, "yyyy-MM-dd HH:mm:ss"),
    )


def trip_2015_raw_schema() -> T.StructType:
    fields = [
        T.StructField("VendorID", T.StringType(), True),
        T.StructField("tpep_pickup_datetime", T.StringType(), True),
        T.StructField("tpep_dropoff_datetime", T.StringType(), True),
        T.StructField("passenger_count", T.StringType(), True),
        T.StructField("trip_distance", T.StringType(), True),
        T.StructField("pickup_longitude", T.StringType(), True),
        T.StructField("pickup_latitude", T.StringType(), True),
        T.StructField("RateCodeID", T.StringType(), True),
        T.StructField("store_and_fwd_flag", T.StringType(), True),
        T.StructField("dropoff_longitude", T.StringType(), True),
        T.StructField("dropoff_latitude", T.StringType(), True),
        T.StructField("payment_type", T.StringType(), True),
        T.StructField("fare_amount", T.StringType(), True),
        T.StructField("extra", T.StringType(), True),
        T.StructField("mta_tax", T.StringType(), True),
        T.StructField("tip_amount", T.StringType(), True),
        T.StructField("tolls_amount", T.StringType(), True),
        T.StructField("improvement_surcharge", T.StringType(), True),
        T.StructField("total_amount", T.StringType(), True),
    ]

    return T.StructType(fields)


def trip_2024_raw_schema() -> T.StructType:
    fields = [
        T.StructField("VendorID", T.StringType(), True),
        T.StructField("tpep_pickup_datetime", T.StringType(), True),
        T.StructField("tpep_dropoff_datetime", T.StringType(), True),
        T.StructField("passenger_count", T.StringType(), True),
        T.StructField("trip_distance", T.StringType(), True),
        T.StructField("RatecodeID", T.StringType(), True),
        T.StructField("store_and_fwd_flag", T.StringType(), True),
        T.StructField("PULocationID", T.StringType(), True),
        T.StructField("DOLocationID", T.StringType(), True),
        T.StructField("payment_type", T.StringType(), True),
        T.StructField("fare_amount", T.StringType(), True),
        T.StructField("extra", T.StringType(), True),
        T.StructField("mta_tax", T.StringType(), True),
        T.StructField("tip_amount", T.StringType(), True),
        T.StructField("tolls_amount", T.StringType(), True),
        T.StructField("improvement_surcharge", T.StringType(), True),
        T.StructField("total_amount", T.StringType(), True),
        T.StructField("congestion_surcharge", T.StringType(), True),
        T.StructField("Airport_fee", T.StringType(), True),
        T.StructField("cbd_congestion_fee", T.StringType(), True),
    ]

    return T.StructType(fields)


def zones_raw_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("LocationID", T.StringType(), True),
            T.StructField("Borough", T.StringType(), True),
            T.StructField("Zone", T.StringType(), True),
            T.StructField("service_zone", T.StringType(), True),
        ]
    )


def clean_trip_2015(df_raw: DataFrame) -> DataFrame:
    _ensure_required_columns(
        df_raw,
        required={
            "vendor": ["VendorID", "vendor_id"],
            "pickup_datetime": ["tpep_pickup_datetime", "pickup_datetime"],
            "dropoff_datetime": ["tpep_dropoff_datetime", "dropoff_datetime"],
            "pickup_longitude": ["pickup_longitude"],
            "pickup_latitude": ["pickup_latitude"],
            "dropoff_longitude": ["dropoff_longitude"],
            "dropoff_latitude": ["dropoff_latitude"],
        },
    )

    def c(*names: str) -> Column:
        found = _first_existing_col_case_insensitive(df_raw, names)
        return F.col(found) if found else F.lit(None)

    df = df_raw.select(
        c("VendorID", "vendor_id").cast("int").alias("vendor_id"),
        _parse_timestamp_multi(c("tpep_pickup_datetime", "pickup_datetime")).alias("pickup_datetime"),
        _parse_timestamp_multi(c("tpep_dropoff_datetime", "dropoff_datetime")).alias("dropoff_datetime"),
        c("passenger_count").cast("int").alias("passenger_count"),
        c("trip_distance").cast("double").alias("trip_distance"),
        c("pickup_longitude").cast("double").alias("pickup_longitude"),
        c("pickup_latitude").cast("double").alias("pickup_latitude"),
        c("RateCodeID", "RatecodeID", "ratecode_id").cast("int").alias("ratecode_id"),
        c("store_and_fwd_flag").alias("store_and_fwd_flag"),
        c("dropoff_longitude").cast("double").alias("dropoff_longitude"),
        c("dropoff_latitude").cast("double").alias("dropoff_latitude"),
        c("payment_type").cast("int").alias("payment_type"),
        c("fare_amount").cast("double").alias("fare_amount"),
        c("extra").cast("double").alias("extra"),
        c("mta_tax").cast("double").alias("mta_tax"),
        c("tip_amount").cast("double").alias("tip_amount"),
        c("tolls_amount").cast("double").alias("tolls_amount"),
        c("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        c("total_amount").cast("double").alias("total_amount"),
    )

    return df


def clean_trip_2024(df_raw: DataFrame) -> DataFrame:
    _ensure_required_columns(
        df_raw,
        required={
            "vendor": ["VendorID", "vendor_id"],
            "pickup_datetime": ["tpep_pickup_datetime", "pickup_datetime"],
            "dropoff_datetime": ["tpep_dropoff_datetime", "dropoff_datetime"],
            "pu_location_id": ["PULocationID", "pu_location_id"],
            "do_location_id": ["DOLocationID", "do_location_id"],
        },
    )

    def c(*names: str) -> Column:
        found = _first_existing_col_case_insensitive(df_raw, names)
        return F.col(found) if found else F.lit(None)

    airport_fee_col = c("airport_fee", "Airport_fee")
    pickup_date_name = _first_existing_col_case_insensitive(df_raw, ["pickup_date"])

    cols = [
        c("VendorID", "vendor_id").cast("int").alias("vendor_id"),
        _parse_timestamp_multi(c("tpep_pickup_datetime", "pickup_datetime")).alias("pickup_datetime"),
        _parse_timestamp_multi(c("tpep_dropoff_datetime", "dropoff_datetime")).alias("dropoff_datetime"),
        c("passenger_count").cast("int").alias("passenger_count"),
        c("trip_distance").cast("double").alias("trip_distance"),
        c("RatecodeID", "RateCodeID", "ratecode_id").cast("int").alias("ratecode_id"),
        c("store_and_fwd_flag").alias("store_and_fwd_flag"),
        c("PULocationID", "pu_location_id").cast("int").alias("pu_location_id"),
        c("DOLocationID", "do_location_id").cast("int").alias("do_location_id"),
        c("payment_type").cast("int").alias("payment_type"),
        c("fare_amount").cast("double").alias("fare_amount"),
        c("extra").cast("double").alias("extra"),
        c("mta_tax").cast("double").alias("mta_tax"),
        c("tip_amount").cast("double").alias("tip_amount"),
        c("tolls_amount").cast("double").alias("tolls_amount"),
        c("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        c("total_amount").cast("double").alias("total_amount"),
        c("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        airport_fee_col.cast("double").alias("airport_fee"),
        c("cbd_congestion_fee").cast("double").alias("cbd_congestion_fee"),
    ]
    if pickup_date_name:
        cols.append(F.col(pickup_date_name).alias("pickup_date"))
    df = df_raw.select(*cols)

    if pickup_date_name:
        return df
    return df.withColumn("pickup_date", F.date_format(F.col("pickup_datetime"), "yyyy-MM-dd"))


def clean_zones(df_raw: DataFrame) -> DataFrame:
    _ensure_required_columns(
        df_raw,
        required={
            "location_id": ["LocationID", "location_id"],
            "borough": ["Borough", "borough"],
            "zone": ["Zone", "zone"],
        },
    )

    def c(*names: str) -> Column:
        found = _first_existing_col_case_insensitive(df_raw, names)
        return F.col(found) if found else F.lit(None)

    return df_raw.select(
        c("LocationID", "location_id").cast("int").alias("location_id"),
        c("Borough", "borough").alias("borough"),
        c("Zone", "zone").alias("zone"),
        c("service_zone", "service_zone").alias("service_zone"),
    )


Q1_RESULT_SCHEMA = T.StructType(
    [
        T.StructField("hour_of_day", T.IntegerType(), False),
        T.StructField("Trips", T.LongType(), False),
        T.StructField("AvgDurationMin", T.DoubleType(), True),
        T.StructField("P90HaversineKm", T.DoubleType(), True),
    ]
)

Q2_RESULT_SCHEMA = T.StructType(
    [
        T.StructField("vendor_id", T.IntegerType(), False),
        T.StructField("date", T.StringType(), False),
        T.StructField("avg_tip_per_mile", T.DoubleType(), True),
    ]
)
