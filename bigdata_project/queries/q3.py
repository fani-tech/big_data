from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from bigdata_project.personalization import PersonalizationParams, filter_2024


def q3_dataframe(trips_2024: DataFrame, zones: DataFrame, params: PersonalizationParams) -> DataFrame:
    k = int(params.k)

    trips_f = filter_2024(trips_2024, params)

    z_pu = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pickup_borough"),
    )

    z_do = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("dropoff_borough"),
    )

    df = (
        trips_f.join(z_pu, on="pu_location_id", how="left")
        .join(z_do, on="do_location_id", how="left")
        .where(
            F.col("pickup_borough").isNotNull()
            & F.col("dropoff_borough").isNotNull()
            & (F.col("pickup_borough") != F.col("dropoff_borough"))
        )
        .groupBy("pickup_borough", "dropoff_borough")
        .agg(F.count(F.lit(1)).alias("trips"))
        .orderBy(F.col("trips").desc())
        .limit(k)
    )

    return df


def q3_sql(spark: SparkSession, trips_2024: DataFrame, zones: DataFrame, params: PersonalizationParams) -> DataFrame:
    k = int(params.k)

    trips_2024.createOrReplaceTempView("trips2024")
    zones.createOrReplaceTempView("zones")

    hours = sorted(int(x) for x in params.hours_set)
    hours_sql = ", ".join(str(x) for x in hours)
    dates_sql = ", ".join(f"'{d}'" for d in params.allowed_2024_pickup_dates)

    query = f"""
    WITH filtered AS (
      SELECT *
      FROM trips2024
      WHERE pickup_date IN ({dates_sql}) AND hour(pickup_datetime) IN ({hours_sql})
    ), pu AS (
      SELECT location_id AS pu_location_id, borough AS pickup_borough FROM zones
    ), do AS (
      SELECT location_id AS do_location_id, borough AS dropoff_borough FROM zones
    )
    SELECT
      pu.pickup_borough AS pickup_borough,
      do.dropoff_borough AS dropoff_borough,
      COUNT(1) AS trips
    FROM filtered t
    LEFT JOIN pu ON t.pu_location_id = pu.pu_location_id
    LEFT JOIN do ON t.do_location_id = do.do_location_id
    WHERE
      pu.pickup_borough IS NOT NULL
      AND do.dropoff_borough IS NOT NULL
      AND pu.pickup_borough <> do.dropoff_borough
    GROUP BY pu.pickup_borough, do.dropoff_borough
    ORDER BY trips DESC
    LIMIT {k}
    """

    return spark.sql(query)
