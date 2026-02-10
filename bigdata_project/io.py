from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from bigdata_project.schemas import (
    clean_trip_2015,
    clean_trip_2024,
    clean_zones,
)


@dataclass(frozen=True)
class LoadedDatasets:
    trip_2015: DataFrame
    trip_2024: DataFrame
    zones: DataFrame


def read_csv(
    spark: SparkSession,
    *,
    path: str,
    schema=None,
    header: bool = True,
) -> DataFrame:
    reader = (
        spark.read.format("csv")
        .option("header", "true" if header else "false")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("escape", '"')
        .option("multiLine", "false")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
    )

    if schema is not None:
        reader = reader.schema(schema)

    return reader.load(path)


def read_parquet(spark: SparkSession, *, path: str) -> DataFrame:
    return spark.read.parquet(path)


def load_trip_2015_csv(spark: SparkSession, *, path: str) -> DataFrame:
    raw = read_csv(spark, path=path, schema=None)
    return clean_trip_2015(raw)


def load_trip_2024_csv(spark: SparkSession, *, path: str) -> DataFrame:
    raw = read_csv(spark, path=path, schema=None)
    return clean_trip_2024(raw)


def load_zones_csv(spark: SparkSession, *, path: str) -> DataFrame:
    raw = read_csv(spark, path=path, schema=None)
    return clean_zones(raw)


def load_trip_2015_parquet(spark: SparkSession, *, path: str) -> DataFrame:
    return clean_trip_2015(read_parquet(spark, path=path))


def load_trip_2024_parquet(spark: SparkSession, *, path: str) -> DataFrame:
    return clean_trip_2024(read_parquet(spark, path=path))


def load_zones_parquet(spark: SparkSession, *, path: str) -> DataFrame:
    return clean_zones(read_parquet(spark, path=path))


def write_parquet(df: DataFrame, *, path: str, mode: str = "overwrite", partition_cols: list[str] | None = None) -> None:
    w = df.write.mode(mode).option("compression", "snappy")
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.parquet(path)


def parquet_dataset_path(parquet_base: str, dataset_name: str) -> str:
    return f"{parquet_base.rstrip('/')}/{dataset_name}"


def convert_all_to_parquet(
    spark: SparkSession,
    *,
    trip_2015_csv_path: str,
    trip_2024_csv_path: str,
    zones_csv_path: str,
    parquet_base: str,
) -> None:
    trip2015 = load_trip_2015_csv(spark, path=trip_2015_csv_path)
    trip2024 = load_trip_2024_csv(spark, path=trip_2024_csv_path)
    zones = load_zones_csv(spark, path=zones_csv_path)

    write_parquet(trip2015, path=parquet_dataset_path(parquet_base, "trip_2015"))
    write_parquet(trip2024, path=parquet_dataset_path(parquet_base, "trip_2024"), partition_cols=["pickup_date"])
    write_parquet(zones, path=parquet_dataset_path(parquet_base, "zones"))


def load_all(
    spark: SparkSession,
    *,
    trip_2015_path: str,
    trip_2024_path: str,
    zones_path: str,
    fmt: str,
) -> LoadedDatasets:
    fmt_lower = fmt.lower()
    if fmt_lower == "csv":
        return LoadedDatasets(
            trip_2015=load_trip_2015_csv(spark, path=trip_2015_path),
            trip_2024=load_trip_2024_csv(spark, path=trip_2024_path),
            zones=load_zones_csv(spark, path=zones_path),
        )
    if fmt_lower == "parquet":
        return LoadedDatasets(
            trip_2015=load_trip_2015_parquet(spark, path=trip_2015_path),
            trip_2024=load_trip_2024_parquet(spark, path=trip_2024_path),
            zones=load_zones_parquet(spark, path=zones_path),
        )

    raise ValueError(f"Άγνωστο format: {fmt}")


def maybe_cache(df: DataFrame, enabled: bool) -> DataFrame:
    if not enabled:
        return df
    return df.cache()


def safe_mkdir_local(path: str) -> None:
    import os

    os.makedirs(path, exist_ok=True)


def _is_hdfs_path(path: str) -> bool:
    return path.startswith("hdfs://")


def ensure_hdfs_dir(spark: SparkSession, *, path: str) -> None:
    if not _is_hdfs_path(path):
        raise ValueError(f"Not an HDFS path: {path}")

    jvm = spark._jvm
    jsc = spark._jsc

    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, jsc.hadoopConfiguration())
    fs.mkdirs(jvm.org.apache.hadoop.fs.Path(path))


def write_text_hdfs(spark: SparkSession, *, path: str, text: str) -> None:
    if not _is_hdfs_path(path):
        raise ValueError(f"Not an HDFS path: {path}")

    jvm = spark._jvm
    jsc = spark._jsc

    uri = jvm.java.net.URI(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, jsc.hadoopConfiguration())

    p = jvm.org.apache.hadoop.fs.Path(path)
    parent = p.getParent()
    if parent is not None:
        fs.mkdirs(parent)

    out = fs.create(p, True)
    try:
        writer = jvm.java.io.BufferedWriter(jvm.java.io.OutputStreamWriter(out, "UTF-8"))
        writer.write(text)
        writer.flush()
        writer.close()
    finally:
        out.close()


def write_text(spark: SparkSession, *, path: str, text: str) -> None:
    if _is_hdfs_path(path):
        write_text_hdfs(spark, path=path, text=text)
    else:
        write_text_local(path, text)


def write_text_local(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def explain_string(df: DataFrame, mode: str = "formatted") -> str:
    qe = df._jdf.queryExecution()

    try:
        jvm = df.sparkSession._jvm
        ExplainMode = jvm.org.apache.spark.sql.execution.ExplainMode
        mode_obj = ExplainMode.fromString(mode)
        return qe.explainString(mode_obj)
    except Exception:
        return qe.explainString(mode)


def physical_plan_string(df: DataFrame) -> str:
    return df._jdf.queryExecution().executedPlan().toString()


def extract_join_operator_raw_names(physical_plan: str) -> list[str]:
    raw: list[str] = []

    candidates = [
        "BroadcastHashJoin",
        "SortMergeJoin",
        "ShuffledHashJoin",
        "BroadcastNestedLoopJoin",
        "CartesianProduct",
    ]

    for c in candidates:
        if c in physical_plan:
            raw.append(c)

    return sorted(set(raw))


def extract_join_operators(physical_plan: str) -> list[str]:
    ops: list[str] = []

    if "BroadcastHashJoin" in physical_plan:
        ops.append("BROADCAST")
    if "SortMergeJoin" in physical_plan:
        ops.append("MERGE")
    if "ShuffledHashJoin" in physical_plan:
        ops.append("SHUFFLE_HASH")
    if "BroadcastNestedLoopJoin" in physical_plan:
        ops.append("BROADCAST_NL")
    if "CartesianProduct" in physical_plan:
        ops.append("SHUFFLE_REPLICATE_NL")

    return sorted(set(ops))


def optional_col(df: DataFrame, col_name: str, default_value: float = 0.0) -> DataFrame:
    if col_name in df.columns:
        return df

    from pyspark.sql import functions as F

    return df.withColumn(col_name, F.lit(default_value).cast("double"))


def ensure_2024_revenue_columns(df: DataFrame) -> DataFrame:
    needed = [
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "extra",
        "mta_tax",
        "congestion_surcharge",
        "airport_fee",
        "total_amount",
    ]

    out = df
    for c in needed:
        out = optional_col(out, c, 0.0)

    return out


def duration_seconds(df: DataFrame, pickup_col: str = "pickup_datetime", dropoff_col: str = "dropoff_datetime") -> DataFrame:
    from pyspark.sql import functions as F

    return df.withColumn("duration_seconds", (F.col(dropoff_col).cast("long") - F.col(pickup_col).cast("long")).cast("long"))


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    from pyspark.sql import functions as F

    r_km = F.lit(6371.0)

    lat1r = F.radians(lat1)
    lon1r = F.radians(lon1)
    lat2r = F.radians(lat2)
    lon2r = F.radians(lon2)

    dlat = lat2r - lat1r
    dlon = lon2r - lon1r

    a = F.pow(F.sin(dlat / 2.0), 2) + F.cos(lat1r) * F.cos(lat2r) * F.pow(F.sin(dlon / 2.0), 2)
    c = 2.0 * F.atan2(F.sqrt(a), F.sqrt(1.0 - a))

    return r_km * c


def format_hour_int(hour: int) -> str:
    return f"{hour:02d}"


def normalize_hour_col(df: DataFrame, hour_col: str = "hour_of_day") -> DataFrame:
    from pyspark.sql import functions as F

    return df.withColumn("hour_str", F.format_string("%02d", F.col(hour_col)))


def load_companies_csv(spark: SparkSession, *, path: str, limit: Optional[int] = None) -> DataFrame:
    df = spark.read.format("csv").option("header", "true").option("mode", "PERMISSIVE").load(path)
    if limit is not None:
        df = df.limit(limit)
    return df


def safe_union_join_keys(left: DataFrame, right: DataFrame, *, left_key: str, right_key: str) -> tuple[DataFrame, DataFrame]:
    from pyspark.sql import functions as F

    l = left
    r = right

    if left_key in l.columns:
        l = l.withColumn(left_key, F.col(left_key).cast("string"))
    if right_key in r.columns:
        r = r.withColumn(right_key, F.col(right_key).cast("string"))

    return l, r


def coalesce_output_path(base_dir: str, *parts: str) -> str:
    base = base_dir.rstrip("/")
    suffix = "/".join(p.strip("/") for p in parts)
    return f"{base}/{suffix}" if suffix else base
