from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from bigdata_project.benchmark import time_action
from bigdata_project.io import (
    coalesce_output_path,
    explain_string,
    extract_join_operator_raw_names,
    extract_join_operators,
    physical_plan_string,
    write_text,
)
from bigdata_project.io import load_trip_2024_parquet, load_zones_parquet
from bigdata_project.personalization import PersonalizationParams, filter_2024


@dataclass(frozen=True)
class JoinStudyResult:
    label: str
    elapsed_seconds: float
    join_operators_mapped: list[str]
    join_operators_raw: list[str]
    explain: str
    physical_plan: str


def run_zone_join_study(
    spark: SparkSession,
    *,
    trips_2024_parquet_path: str,
    zones_parquet_path: str,
    params: PersonalizationParams,
    output_base_dir: str,
    zones_limit: int = 50,
    disable_broadcast: bool = False,
    disable_aqe: bool = False,
) -> JoinStudyResult:
    trips = load_trip_2024_parquet(spark, path=trips_2024_parquet_path)
    zones = load_zones_parquet(spark, path=zones_parquet_path)

    trips_f = filter_2024(trips, params)
    zones_small = zones.orderBy("location_id").limit(int(zones_limit))

    prev_threshold: Optional[str] = None
    prev_aqe: Optional[str] = None
    if disable_broadcast:
        prev_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", None)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    if disable_aqe:
        prev_aqe = spark.conf.get("spark.sql.adaptive.enabled", None)
        spark.conf.set("spark.sql.adaptive.enabled", "false")

    label_parts: list[str] = []
    label_parts.append("no_broadcast" if disable_broadcast else "default")
    if disable_aqe:
        label_parts.append("no_aqe")
    label = "_".join(label_parts)

    try:
        df: DataFrame = trips_f.join(
            zones_small,
            trips_f.pu_location_id == zones_small.location_id,
            how="left",
        ).select(
            trips_f["*"],
            zones_small["borough"],
            zones_small["zone"],
            zones_small["service_zone"],
        )

        elapsed = time_action(lambda: df.count())

        expl = explain_string(df, mode="formatted")
        phys = physical_plan_string(df)
        ops_mapped = extract_join_operators(phys)
        ops_raw = extract_join_operator_raw_names(phys)

        write_text(spark, path=coalesce_output_path(output_base_dir, "explain", f"join_study_{label}_explain.txt"), text=expl)
        write_text(spark, path=coalesce_output_path(output_base_dir, "explain", f"join_study_{label}_physical_plan.txt"), text=phys)

        write_text(
            spark,
            path=coalesce_output_path(output_base_dir, "explain", f"join_study_{label}_summary.txt"),
            text="\n".join(
                [
                    f"label: {label}",
                    f"elapsed_seconds: {elapsed:.6f}",
                    f"join_operators_mapped: {', '.join(ops_mapped) if ops_mapped else '(none detected)'}",
                    f"join_operators_raw: {', '.join(ops_raw) if ops_raw else '(none detected)'}",
                    "",
                    "(Το πλήρες explain/plan έχουν γραφτεί σε ξεχωριστά αρχεία.)",
                ]
            )
            + "\n",
        )

        return JoinStudyResult(
            label=label,
            elapsed_seconds=float(elapsed),
            join_operators_mapped=ops_mapped,
            join_operators_raw=ops_raw,
            explain=expl,
            physical_plan=phys,
        )

    finally:
        if disable_broadcast:
            if prev_threshold is not None:
                spark.conf.set("spark.sql.autoBroadcastJoinThreshold", prev_threshold)
            else:
                spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

        if disable_aqe:
            if prev_aqe is not None:
                spark.conf.set("spark.sql.adaptive.enabled", prev_aqe)
            else:
                spark.conf.unset("spark.sql.adaptive.enabled")


def run_zone_join_study_two_runs(
    spark: SparkSession,
    *,
    trips_2024_parquet_path: str,
    zones_parquet_path: str,
    params: PersonalizationParams,
    output_base_dir: str,
    zones_limit: int = 50,
) -> tuple[JoinStudyResult, JoinStudyResult]:
    r1 = run_zone_join_study(
        spark,
        trips_2024_parquet_path=trips_2024_parquet_path,
        zones_parquet_path=zones_parquet_path,
        params=params,
        output_base_dir=output_base_dir,
        zones_limit=zones_limit,
        disable_broadcast=False,
        disable_aqe=False,
    )

    r2 = run_zone_join_study(
        spark,
        trips_2024_parquet_path=trips_2024_parquet_path,
        zones_parquet_path=zones_parquet_path,
        params=params,
        output_base_dir=output_base_dir,
        zones_limit=zones_limit,
        disable_broadcast=True,
        disable_aqe=True,
    )

    return r1, r2
