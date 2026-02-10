from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bigdata_project.io import ensure_2024_revenue_columns
from bigdata_project.personalization import PersonalizationParams, filter_2024


def q6_dataframe(trips_2024: DataFrame, zones: DataFrame, params: PersonalizationParams) -> DataFrame:
    trips_2024 = ensure_2024_revenue_columns(trips_2024)
    trips_f = filter_2024(trips_2024, params)

    z_pu = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("service_zone").alias("pickup_service_zone"),
    )

    df = trips_f.join(z_pu, on="pu_location_id", how="left").where(F.col("pickup_service_zone").isNotNull())

    total_revenue = F.sum(F.coalesce(F.col("total_amount"), F.lit(0.0))).alias("TotalRevenue")
    congestion_airport = F.sum(
        F.coalesce(F.col("congestion_surcharge"), F.lit(0.0)) + F.coalesce(F.col("airport_fee"), F.lit(0.0))
    ).alias("CongestionAirport")

    out = (
        df.groupBy("vendor_id", "pickup_service_zone")
        .agg(
            F.count(F.lit(1)).alias("Trips"),
            total_revenue,
            congestion_airport,
        )
        .withColumn(
            "Share",
            F.when(F.col("TotalRevenue") > 0, (F.col("CongestionAirport") / F.col("TotalRevenue")).cast("double")).otherwise(F.lit(None)),
        )
        .withColumn("AvgRevenuePerTrip", (F.col("TotalRevenue") / F.col("Trips")).cast("double"))
        .orderBy("vendor_id", "pickup_service_zone")
    )

    return out
