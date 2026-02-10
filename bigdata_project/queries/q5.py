from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from bigdata_project.personalization import PersonalizationParams, filter_2024


def q5_dataframe(
    trips_2024: DataFrame,
    zones: DataFrame,
    params: PersonalizationParams,
    *,
    limit: int | None = None,
    broadcast_zones: bool = False,
) -> DataFrame:
    k = int(limit) if limit is not None else int(params.k)

    trips_f = filter_2024(trips_2024, params)

    z = zones.select(
        F.col("location_id").alias("location_id"),
        F.col("borough").alias("borough"),
        F.col("zone").alias("zone"),
    )
    if broadcast_zones:
        z = F.broadcast(z)

    pickups = trips_f.groupBy("pu_location_id").agg(F.count(F.lit(1)).alias("pickups"))
    dropoffs = trips_f.groupBy("do_location_id").agg(F.count(F.lit(1)).alias("dropoffs"))

    pu = pickups.join(z, pickups.pu_location_id == z.location_id, how="left").select(
        z.location_id.alias("location_id"),
        z.borough.alias("borough"),
        z.zone.alias("zone"),
        F.col("pickups"),
    )

    do = dropoffs.join(z, dropoffs.do_location_id == z.location_id, how="left").select(
        z.location_id.alias("location_id"),
        z.borough.alias("borough"),
        z.zone.alias("zone"),
        F.col("dropoffs"),
    )

    merged = pu.join(do, on=["location_id", "borough", "zone"], how="full_outer")

    merged = (
        merged.withColumn("pickups", F.coalesce(F.col("pickups"), F.lit(0)).cast("long"))
        .withColumn("dropoffs", F.coalesce(F.col("dropoffs"), F.lit(0)).cast("long"))
        .withColumn("max_pd", F.greatest(F.col("pickups"), F.col("dropoffs")).cast("double"))
        .withColumn("min_pd", F.least(F.col("pickups"), F.col("dropoffs")).cast("double"))
        .withColumn("den", F.greatest(F.lit(1.0), F.col("min_pd")))
        .withColumn("imbalance", (F.col("max_pd") / F.col("den")).cast("double"))
    )

    out = (
        merged.where(F.col("borough").isNotNull() & F.col("zone").isNotNull())
        .select("borough", "zone", "pickups", "dropoffs", "imbalance")
        .orderBy(F.col("imbalance").desc())
        .limit(k)
    )

    return out
