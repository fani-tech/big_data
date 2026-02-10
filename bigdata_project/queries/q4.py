from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from bigdata_project.personalization import PersonalizationParams


def q4_sql(spark: SparkSession, trips_2024: DataFrame, zones: DataFrame, params: PersonalizationParams) -> DataFrame:
    k_dates = ", ".join(f"'{d}'" for d in params.allowed_2024_pickup_dates)
    hours = sorted(int(x) for x in params.hours_set)
    hours_sql = ", ".join(str(x) for x in hours)

    trips_2024.createOrReplaceTempView("trips2024")
    zones.createOrReplaceTempView("zones")

    query = f"""
    WITH filtered AS (
      SELECT *
      FROM trips2024
      WHERE pickup_date IN ({k_dates}) AND hour(pickup_datetime) IN ({hours_sql})
    ), pu AS (
      SELECT location_id AS pu_location_id, borough AS pickup_borough
      FROM zones
    )
    SELECT
      pu.pickup_borough AS pickup_borough,
      COUNT(1) AS Trips,
      AVG(CASE WHEN t.payment_type = 1 THEN 1.0 ELSE 0.0 END) AS card_share,
      AVG(CASE WHEN t.payment_type = 1 AND t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) ELSE NULL END) AS avg_tip_rate_card
    FROM filtered t
    LEFT JOIN pu ON t.pu_location_id = pu.pu_location_id
    WHERE pu.pickup_borough IS NOT NULL
    GROUP BY pu.pickup_borough
    ORDER BY avg_tip_rate_card DESC NULLS LAST
    """

    return spark.sql(query)
