from __future__ import annotations

from typing import Mapping, Optional

from pyspark.sql import SparkSession


def build_spark_session(
    *,
    app_name: str,
    master: Optional[str] = None,
    timezone: str = "UTC",
    shuffle_partitions: Optional[int] = None,
    eventlog_dir: Optional[str] = None,
    extra_conf: Optional[Mapping[str, str]] = None,
) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    if master:
        builder = builder.master(master)

    builder = builder.config("spark.sql.session.timeZone", timezone)

    import sys

    builder = builder.config("spark.pyspark.python", sys.executable).config("spark.pyspark.driver.python", sys.executable)

    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))

    if eventlog_dir:
        builder = builder.config("spark.eventLog.enabled", "true").config("spark.eventLog.dir", eventlog_dir)

    if extra_conf:
        for k, v in extra_conf.items():
            builder = builder.config(k, v)

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark
