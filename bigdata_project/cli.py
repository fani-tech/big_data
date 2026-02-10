from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from bigdata_project.benchmark import (
    TimingRecord,
    maybe_warmup,
    plot_timings_per_query,
    run_repeated,
    save_summary_txt,
    save_timings_json,
    time_action,
)
from bigdata_project.config import (
    AppConfig,
    StorageBackend,
    default_dataset_locations,
    default_output_locations,
)
from bigdata_project.io import (
    coalesce_output_path,
    convert_all_to_parquet,
    ensure_hdfs_dir,
    explain_string,
    extract_join_operator_raw_names,
    extract_join_operators,
    load_all,
    load_trip_2015_csv,
    load_trip_2015_parquet,
    load_trip_2024_csv,
    load_trip_2024_parquet,
    load_zones_csv,
    load_zones_parquet,
    parquet_dataset_path,
    physical_plan_string,
    safe_mkdir_local,
    write_text,
)
from bigdata_project.joins_study import run_zone_join_study_two_runs
from bigdata_project.personalization import params_from_am
from bigdata_project.spark import build_spark_session
from bigdata_project.queries.q1 import q1_dataframe, q1_dataframe_udf, q1_rdd
from bigdata_project.queries.q2 import q2_dataframe, q2_rdd, q2_sql
from bigdata_project.queries.q3 import q3_dataframe, q3_sql
from bigdata_project.queries.q4 import q4_sql
from bigdata_project.queries.q5 import q5_dataframe
from bigdata_project.queries.q6 import q6_dataframe


def _build_config(args: argparse.Namespace) -> AppConfig:
    storage = StorageBackend(args.storage)

    datasets = default_dataset_locations(storage, hdfs_base=args.hdfs_base)

    outputs = default_output_locations(
        storage,
        hdfs_base=args.hdfs_base,
        hdfs_user=args.hdfs_user,
        parquet_base_override=args.parquet_base,
        eventlog_dir_override=args.eventlog_dir,
        results_base_hdfs_override=args.hdfs_results_base,
        results_dir_local=Path(args.results_dir),
    )

    return AppConfig(storage=storage, datasets=datasets, outputs=outputs)


def _ensure_local_results_dirs(cfg: AppConfig) -> None:
    cfg.outputs.results_dir_local.mkdir(parents=True, exist_ok=True)
    safe_mkdir_local(str(cfg.outputs.results_dir_local / "explain"))
    safe_mkdir_local(str(cfg.outputs.results_dir_local / "plots"))
    safe_mkdir_local(str(cfg.outputs.results_dir_local / "outputs"))

    if cfg.outputs.eventlog_dir and not cfg.outputs.eventlog_dir.startswith("hdfs://"):
        safe_mkdir_local(cfg.outputs.eventlog_dir)


def _require_personalization(args: argparse.Namespace):
    if args.am is None:
        raise ValueError("Λείπει --am (ΑΜ). Είναι υποχρεωτικό για τα εξατομικευμένα queries.")
    return params_from_am(int(args.am))


def _personalization_payload(params) -> str:
    hours = ", ".join(str(h) for h in sorted(params.hours_set))
    days = ", ".join(sorted({d.split("-")[2] for d in params.allowed_2024_pickup_dates}))
    return (
        f"am={params.am}\n"
        f"h={params.h}\n"
        f"d={params.d}\n"
        f"K={params.k}\n"
        f"L={params.L}\n"
        f"hours_set={{{hours}}}\n"
        f"day_of_month_set={{{days}}}\n"
    )


def _save_personalization(cfg: AppConfig, *, spark, params, hdfs_results_dir: str | None) -> None:
    payload = _personalization_payload(params)
    write_text(spark, path=str(cfg.outputs.results_dir_local / "personalization.txt"), text=payload)
    if hdfs_results_dir:
        write_text(spark, path=coalesce_output_path(hdfs_results_dir, "personalization.txt"), text=payload)


def _save_explain(cfg: AppConfig, *, name: str, df, hdfs_results_dir: str | None = None) -> None:
    spark = df.sparkSession

    explain_txt = explain_string(df, mode="formatted")
    physical_txt = physical_plan_string(df)

    write_text(spark, path=str(cfg.outputs.results_dir_local / "explain" / f"{name}_explain.txt"), text=explain_txt)
    write_text(spark, path=str(cfg.outputs.results_dir_local / "explain" / f"{name}_physical.txt"), text=physical_txt)

    join_ops = extract_join_operators(physical_txt)
    join_ops_raw = extract_join_operator_raw_names(physical_txt)

    summary_lines = [
        f"name: {name}",
        f"join_operators_mapped: {', '.join(join_ops) if join_ops else '(none detected)'}",
        f"join_operators_raw: {', '.join(join_ops_raw) if join_ops_raw else '(none detected)'}",
    ]
    summary_txt = "\n".join(summary_lines) + "\n"
    write_text(spark, path=str(cfg.outputs.results_dir_local / "explain" / f"{name}_summary.txt"), text=summary_txt)

    if hdfs_results_dir:
        write_text(spark, path=coalesce_output_path(hdfs_results_dir, "explain", f"{name}_explain.txt"), text=explain_txt)
        write_text(spark, path=coalesce_output_path(hdfs_results_dir, "explain", f"{name}_physical.txt"), text=physical_txt)
        write_text(spark, path=coalesce_output_path(hdfs_results_dir, "explain", f"{name}_summary.txt"), text=summary_txt)


def _hdfs_results_dir(cfg: AppConfig, *, spark, category: str) -> str | None:
    if not cfg.outputs.results_base_hdfs:
        return None

    run_id = spark.sparkContext.applicationId
    return coalesce_output_path(cfg.outputs.results_base_hdfs, category, run_id)


def _save_output_df(cfg: AppConfig, *, category: str, name: str, df, hdfs_results_dir: str | None = None) -> None:
    spark = df.sparkSession
    run_id = spark.sparkContext.applicationId

    local_dir = cfg.outputs.results_dir_local / "outputs" / category / run_id / name
    local_dir.parent.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(local_dir))

    if hdfs_results_dir:
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(coalesce_output_path(hdfs_results_dir, "outputs", name))

def cmd_convert(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name="convert_to_parquet",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        elapsed = time_action(
            lambda: convert_all_to_parquet(
                spark,
                trip_2015_csv_path=cfg.datasets.trip_2015_csv,
                trip_2024_csv_path=cfg.datasets.trip_2024_csv,
                zones_csv_path=cfg.datasets.zones_csv,
                parquet_base=cfg.outputs.parquet_base,
            )
        )

        out_txt = cfg.outputs.results_dir_local / "conversion_time.txt"
        out_payload = f"conversion_elapsed_seconds={elapsed:.6f}\n"
        out_txt.write_text(out_payload, encoding="utf-8")

        if cfg.outputs.results_base_hdfs:
            run_id = spark.sparkContext.applicationId
            hdfs_results_dir = coalesce_output_path(cfg.outputs.results_base_hdfs, "convert", run_id)
            write_text(spark, path=coalesce_output_path(hdfs_results_dir, "conversion_time.txt"), text=out_payload)

    finally:
        spark.stop()


def _maybe_convert_all(cfg: AppConfig, *, spark, enabled: bool) -> None:
    if not enabled:
        return

    convert_all_to_parquet(
        spark,
        trip_2015_csv_path=cfg.datasets.trip_2015_csv,
        trip_2024_csv_path=cfg.datasets.trip_2024_csv,
        zones_csv_path=cfg.datasets.zones_csv,
        parquet_base=cfg.outputs.parquet_base,
    )


def cmd_run_all(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name="bigdata_project_run_all",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    timings: list[TimingRecord] = []

    hdfs_results_dir: str | None = None
    if cfg.outputs.results_base_hdfs:
        run_id = spark.sparkContext.applicationId
        hdfs_results_dir = coalesce_output_path(cfg.outputs.results_base_hdfs, "run-all", run_id)

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        if not args.skip_convert:
            elapsed = time_action(
                lambda: convert_all_to_parquet(
                    spark,
                    trip_2015_csv_path=cfg.datasets.trip_2015_csv,
                    trip_2024_csv_path=cfg.datasets.trip_2024_csv,
                    zones_csv_path=cfg.datasets.zones_csv,
                    parquet_base=cfg.outputs.parquet_base,
                )
            )
            timings.append(
                TimingRecord(
                    label="convert_to_parquet",
                    query_id="CONVERT",
                    api="DATAFRAME",
                    input_format="csv",
                    elapsed_seconds=float(elapsed),
                    run_index=0,
                )
            )

        trip2015_parquet = parquet_dataset_path(cfg.outputs.parquet_base, "trip_2015")
        trip2024_parquet = parquet_dataset_path(cfg.outputs.parquet_base, "trip_2024")
        zones_parquet = parquet_dataset_path(cfg.outputs.parquet_base, "zones")

        ds_csv = load_all(
            spark,
            trip_2015_path=cfg.datasets.trip_2015_csv,
            trip_2024_path=cfg.datasets.trip_2024_csv,
            zones_path=cfg.datasets.zones_csv,
            fmt="csv",
        )

        ds_parquet = load_all(
            spark,
            trip_2015_path=trip2015_parquet,
            trip_2024_path=trip2024_parquet,
            zones_path=zones_parquet,
            fmt="parquet",
        )

        repeats = int(args.repeats)
        do_warmup = bool(args.warmup)
        params = _require_personalization(args)
        _save_personalization(cfg, spark=spark, params=params, hdfs_results_dir=hdfs_results_dir)

        maybe_warmup(lambda: q1_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params).count(), do_warmup)
        timings += run_repeated(
            "Q1_RDD_csv",
            "Q1",
            "RDD",
            "csv",
            lambda: q1_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params).count(),
            repeats=repeats,
        )
        q1_rdd_df = q1_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params)

        for fmt, trip_df in [("csv", ds_csv.trip_2015), ("parquet", ds_parquet.trip_2015)]:
            q1_df = q1_dataframe(trip_df, params)
            maybe_warmup(lambda: q1_df.count(), do_warmup)
            timings += run_repeated(f"Q1_DF_{fmt}", "Q1", "DATAFRAME", fmt, lambda: q1_df.count(), repeats=repeats)
            _save_explain(cfg, name=f"Q1_DF_{fmt}", df=q1_df, hdfs_results_dir=hdfs_results_dir)

        for fmt, trip_df in [("csv", ds_csv.trip_2015), ("parquet", ds_parquet.trip_2015)]:
            q1_df_udf = q1_dataframe_udf(trip_df, params)
            maybe_warmup(lambda: q1_df_udf.count(), do_warmup)
            timings += run_repeated(f"Q1_DF_UDF_{fmt}", "Q1", "DATAFRAME_UDF", fmt, lambda: q1_df_udf.count(), repeats=repeats)
            _save_explain(cfg, name=f"Q1_DF_UDF_{fmt}", df=q1_df_udf, hdfs_results_dir=hdfs_results_dir)

        maybe_warmup(lambda: q2_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params).count(), do_warmup)
        timings += run_repeated(
            "Q2_RDD_csv",
            "Q2",
            "RDD",
            "csv",
            lambda: q2_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params).count(),
            repeats=repeats,
        )
        q2_rdd_df = q2_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params)

        for fmt, trip_df in [("csv", ds_csv.trip_2015), ("parquet", ds_parquet.trip_2015)]:
            q2_df = q2_dataframe(trip_df, params)
            maybe_warmup(lambda: q2_df.count(), do_warmup)
            timings += run_repeated(f"Q2_DF_{fmt}", "Q2", "DATAFRAME", fmt, lambda: q2_df.count(), repeats=repeats)
            _save_explain(cfg, name=f"Q2_DF_{fmt}", df=q2_df, hdfs_results_dir=hdfs_results_dir)

        for fmt, trip_df in [("csv", ds_csv.trip_2015), ("parquet", ds_parquet.trip_2015)]:
            q2_sql_df = q2_sql(spark, trip_df, params)
            maybe_warmup(lambda: q2_sql_df.count(), do_warmup)
            timings += run_repeated(f"Q2_SQL_{fmt}", "Q2", "SQL", fmt, lambda: q2_sql_df.count(), repeats=repeats)
            _save_explain(cfg, name=f"Q2_SQL_{fmt}", df=q2_sql_df, hdfs_results_dir=hdfs_results_dir)

        for fmt, ds in [("csv", ds_csv), ("parquet", ds_parquet)]:
            q3_df = q3_dataframe(ds.trip_2024, ds.zones, params)
            maybe_warmup(lambda: q3_df.count(), do_warmup)
            timings += run_repeated(f"Q3_DF_{fmt}", "Q3", "DATAFRAME", fmt, lambda: q3_df.count(), repeats=repeats)
            _save_explain(cfg, name=f"Q3_DF_{fmt}", df=q3_df, hdfs_results_dir=hdfs_results_dir)

            q3_sql_df = q3_sql(spark, ds.trip_2024, ds.zones, params)
            maybe_warmup(lambda: q3_sql_df.count(), do_warmup)
            timings += run_repeated(f"Q3_SQL_{fmt}", "Q3", "SQL", fmt, lambda: q3_sql_df.count(), repeats=repeats)
            _save_explain(cfg, name=f"Q3_SQL_{fmt}", df=q3_sql_df, hdfs_results_dir=hdfs_results_dir)

        for fmt, ds in [("csv", ds_csv), ("parquet", ds_parquet)]:
            q4_df = q4_sql(spark, ds.trip_2024, ds.zones, params)
            maybe_warmup(lambda: q4_df.count(), do_warmup)
            timings += run_repeated(f"Q4_SQL_{fmt}", "Q4", "SQL", fmt, lambda: q4_df.count(), repeats=repeats)
            _save_explain(cfg, name=f"Q4_SQL_{fmt}", df=q4_df, hdfs_results_dir=hdfs_results_dir)

        q5_df_default = q5_dataframe(ds_parquet.trip_2024, ds_parquet.zones, params)
        maybe_warmup(lambda: q5_df_default.count(), do_warmup)
        timings += run_repeated(
            "Q5_DF_default_parquet",
            "Q5",
            "DATAFRAME",
            "parquet",
            lambda: q5_df_default.count(),
            repeats=repeats,
        )
        _save_explain(cfg, name="Q5_DF_default_parquet", df=q5_df_default, hdfs_results_dir=hdfs_results_dir)

        prev_threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold", None)
        prev_aqe = spark.conf.get("spark.sql.adaptive.enabled", None)
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        try:
            q5_df_no_broadcast = q5_dataframe(ds_parquet.trip_2024, ds_parquet.zones, params)
            maybe_warmup(lambda: q5_df_no_broadcast.count(), do_warmup)
            timings += run_repeated(
                "Q5_DF_no_broadcast_parquet",
                "Q5",
                "DATAFRAME",
                "parquet",
                lambda: q5_df_no_broadcast.count(),
                repeats=repeats,
            )
            _save_explain(cfg, name="Q5_DF_no_broadcast_parquet", df=q5_df_no_broadcast, hdfs_results_dir=hdfs_results_dir)
        finally:
            if prev_threshold is None:
                spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
            else:
                spark.conf.set("spark.sql.autoBroadcastJoinThreshold", prev_threshold)

            if prev_aqe is None:
                spark.conf.unset("spark.sql.adaptive.enabled")
            else:
                spark.conf.set("spark.sql.adaptive.enabled", prev_aqe)

        q6_input = ds_parquet if args.q6_input_format == "parquet" else ds_csv
        q6_df = q6_dataframe(q6_input.trip_2024, q6_input.zones, params)
        maybe_warmup(lambda: q6_df.count(), do_warmup)
        timings += run_repeated(
            f"Q6_DF_{args.q6_input_format}",
            "Q6",
            "DATAFRAME",
            args.q6_input_format,
            lambda: q6_df.count(),
            repeats=repeats,
        )
        _save_explain(cfg, name=f"Q6_DF_{args.q6_input_format}", df=q6_df, hdfs_results_dir=hdfs_results_dir)

        timings_path = cfg.outputs.results_dir_local / "timings.json"
        save_timings_json(timings_path, timings)

        summary_path = cfg.outputs.results_dir_local / "timings_summary.txt"
        save_summary_txt(summary_path, timings)

        if hdfs_results_dir:
            write_text(spark, path=coalesce_output_path(hdfs_results_dir, "timings.json"), text=timings_path.read_text(encoding="utf-8"))
            write_text(spark, path=coalesce_output_path(hdfs_results_dir, "timings_summary.txt"), text=summary_path.read_text(encoding="utf-8"))

            out_base = coalesce_output_path(hdfs_results_dir, "outputs")

            outputs: list[tuple[str, object]] = [
                ("Q1_RDD_csv", q1_rdd_df),
                ("Q1_DF_csv", q1_dataframe(ds_csv.trip_2015, params)),
                ("Q1_DF_parquet", q1_dataframe(ds_parquet.trip_2015, params)),
                ("Q1_DF_UDF_csv", q1_dataframe_udf(ds_csv.trip_2015, params)),
                ("Q1_DF_UDF_parquet", q1_dataframe_udf(ds_parquet.trip_2015, params)),
                ("Q2_RDD_csv", q2_rdd_df),
                ("Q2_DF_csv", q2_dataframe(ds_csv.trip_2015, params)),
                ("Q2_DF_parquet", q2_dataframe(ds_parquet.trip_2015, params)),
                ("Q2_SQL_csv", q2_sql(spark, ds_csv.trip_2015, params)),
                ("Q2_SQL_parquet", q2_sql(spark, ds_parquet.trip_2015, params)),
                ("Q3_DF_csv", q3_dataframe(ds_csv.trip_2024, ds_csv.zones, params)),
                ("Q3_DF_parquet", q3_dataframe(ds_parquet.trip_2024, ds_parquet.zones, params)),
                ("Q3_SQL_csv", q3_sql(spark, ds_csv.trip_2024, ds_csv.zones, params)),
                ("Q3_SQL_parquet", q3_sql(spark, ds_parquet.trip_2024, ds_parquet.zones, params)),
                ("Q4_SQL_csv", q4_sql(spark, ds_csv.trip_2024, ds_csv.zones, params)),
                ("Q4_SQL_parquet", q4_sql(spark, ds_parquet.trip_2024, ds_parquet.zones, params)),
                ("Q5_DF_default_parquet", q5_df_default),
                ("Q5_DF_no_broadcast_parquet", q5_df_no_broadcast),
                ("Q6_DF_csv", q6_dataframe(ds_csv.trip_2024, ds_csv.zones, params)),
                ("Q6_DF_parquet", q6_dataframe(ds_parquet.trip_2024, ds_parquet.zones, params)),
            ]

            for name, df in outputs:
                df.coalesce(1).write.mode("overwrite").option("header", "true").csv(coalesce_output_path(out_base, name))

    finally:
        spark.stop()


def _write_timing_artifact(cfg: AppConfig, *, spark, category: str, name: str, elapsed_seconds: float, hdfs_results_dir: str | None) -> None:
    payload = f"elapsed_seconds={elapsed_seconds:.6f}\n"

    local_dir = cfg.outputs.results_dir_local / "outputs" / category / spark.sparkContext.applicationId
    local_dir.mkdir(parents=True, exist_ok=True)
    write_text(spark, path=str(local_dir / f"{name}_timing.txt"), text=payload)

    if hdfs_results_dir:
        write_text(spark, path=coalesce_output_path(hdfs_results_dir, f"{name}_timing.txt"), text=payload)


def cmd_q1(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    if args.api == "rdd" and args.fmt != "csv":
        raise ValueError("Q1: το RDD/MR τρέχει μόνο πάνω σε CSV (όπως ζητά η εκφώνηση).")

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name=f"Q1_{args.api}_{args.fmt}",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        _maybe_convert_all(cfg, spark=spark, enabled=bool(args.ensure_parquet))

        hdfs_results_dir = _hdfs_results_dir(cfg, spark=spark, category="q1")
        params = _require_personalization(args)
        _save_personalization(cfg, spark=spark, params=params, hdfs_results_dir=hdfs_results_dir)

        if args.api == "rdd":
            name = "Q1_RDD_csv"
            elapsed = time_action(lambda: q1_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params).count())
            df = q1_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params)
        else:
            trips = (
                load_trip_2015_csv(spark, path=cfg.datasets.trip_2015_csv)
                if args.fmt == "csv"
                else load_trip_2015_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "trip_2015"))
            )

            if args.api == "df":
                df = q1_dataframe(trips, params)
                name = f"Q1_DF_{args.fmt}"
            else:
                df = q1_dataframe_udf(trips, params)
                name = f"Q1_DF_UDF_{args.fmt}"
            elapsed = time_action(lambda: df.count())

        _save_explain(cfg, name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _save_output_df(cfg, category="q1", name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _write_timing_artifact(cfg, spark=spark, category="q1", name=name, elapsed_seconds=elapsed, hdfs_results_dir=hdfs_results_dir)

    finally:
        spark.stop()


def cmd_q2(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    if args.api == "rdd" and args.fmt != "csv":
        raise ValueError("Q2: το RDD/MR τρέχει μόνο πάνω σε CSV (όπως ζητά η εκφώνηση).")

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name=f"Q2_{args.api}_{args.fmt}",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        _maybe_convert_all(cfg, spark=spark, enabled=bool(args.ensure_parquet))

        hdfs_results_dir = _hdfs_results_dir(cfg, spark=spark, category="q2")
        params = _require_personalization(args)
        _save_personalization(cfg, spark=spark, params=params, hdfs_results_dir=hdfs_results_dir)

        if args.api == "rdd":
            name = "Q2_RDD_csv"
            elapsed = time_action(lambda: q2_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params).count())
            df = q2_rdd(spark, csv_path=cfg.datasets.trip_2015_csv, params=params)
        else:
            trips = (
                load_trip_2015_csv(spark, path=cfg.datasets.trip_2015_csv)
                if args.fmt == "csv"
                else load_trip_2015_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "trip_2015"))
            )

            if args.api == "df":
                df = q2_dataframe(trips, params)
                name = f"Q2_DF_{args.fmt}"
            else:
                df = q2_sql(spark, trips, params)
                name = f"Q2_SQL_{args.fmt}"
            elapsed = time_action(lambda: df.count())

        _save_explain(cfg, name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _save_output_df(cfg, category="q2", name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _write_timing_artifact(cfg, spark=spark, category="q2", name=name, elapsed_seconds=elapsed, hdfs_results_dir=hdfs_results_dir)

    finally:
        spark.stop()


def cmd_q3(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name=f"Q3_{args.api}_{args.fmt}",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        _maybe_convert_all(cfg, spark=spark, enabled=bool(args.ensure_parquet))

        hdfs_results_dir = _hdfs_results_dir(cfg, spark=spark, category="q3")
        params = _require_personalization(args)
        _save_personalization(cfg, spark=spark, params=params, hdfs_results_dir=hdfs_results_dir)

        if args.fmt == "csv":
            trips = load_trip_2024_csv(spark, path=cfg.datasets.trip_2024_csv)
            zones = load_zones_csv(spark, path=cfg.datasets.zones_csv)
        else:
            trips = load_trip_2024_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "trip_2024"))
            zones = load_zones_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "zones"))

        if args.api == "df":
            df = q3_dataframe(trips, zones, params)
            name = f"Q3_DF_{args.fmt}"
        else:
            df = q3_sql(spark, trips, zones, params)
            name = f"Q3_SQL_{args.fmt}"

        elapsed = time_action(lambda: df.count())

        _save_explain(cfg, name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _save_output_df(cfg, category="q3", name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _write_timing_artifact(cfg, spark=spark, category="q3", name=name, elapsed_seconds=elapsed, hdfs_results_dir=hdfs_results_dir)

    finally:
        spark.stop()


def cmd_q4(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name=f"Q4_sql_{args.fmt}",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        _maybe_convert_all(cfg, spark=spark, enabled=bool(args.ensure_parquet))

        hdfs_results_dir = _hdfs_results_dir(cfg, spark=spark, category="q4")
        params = _require_personalization(args)
        _save_personalization(cfg, spark=spark, params=params, hdfs_results_dir=hdfs_results_dir)

        trips = (
            load_trip_2024_csv(spark, path=cfg.datasets.trip_2024_csv)
            if args.fmt == "csv"
            else load_trip_2024_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "trip_2024"))
        )
        zones = load_zones_csv(spark, path=cfg.datasets.zones_csv) if args.fmt == "csv" else load_zones_parquet(
            spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "zones")
        )

        df = q4_sql(spark, trips, zones, params)
        name = f"Q4_SQL_{args.fmt}"

        elapsed = time_action(lambda: df.count())

        _save_explain(cfg, name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _save_output_df(cfg, category="q4", name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _write_timing_artifact(cfg, spark=spark, category="q4", name=name, elapsed_seconds=elapsed, hdfs_results_dir=hdfs_results_dir)

    finally:
        spark.stop()


def cmd_q5(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name=f"Q5_df_{args.fmt}",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        _maybe_convert_all(cfg, spark=spark, enabled=bool(args.ensure_parquet))

        hdfs_results_dir = _hdfs_results_dir(cfg, spark=spark, category="q5")
        params = _require_personalization(args)
        _save_personalization(cfg, spark=spark, params=params, hdfs_results_dir=hdfs_results_dir)

        if args.fmt == "csv":
            trips = load_trip_2024_csv(spark, path=cfg.datasets.trip_2024_csv)
            zones = load_zones_csv(spark, path=cfg.datasets.zones_csv)
        else:
            trips = load_trip_2024_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "trip_2024"))
            zones = load_zones_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "zones"))

        limit = int(args.limit) if args.limit is not None else None
        df = q5_dataframe(trips, zones, params, limit=limit)
        name = f"Q5_DF_{args.fmt}_top{limit if limit is not None else int(params.k)}"

        elapsed = time_action(lambda: df.count())

        _save_explain(cfg, name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _save_output_df(cfg, category="q5", name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _write_timing_artifact(cfg, spark=spark, category="q5", name=name, elapsed_seconds=elapsed, hdfs_results_dir=hdfs_results_dir)

    finally:
        spark.stop()


def cmd_q6(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name=f"Q6_df_{args.fmt}",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        _maybe_convert_all(cfg, spark=spark, enabled=bool(args.ensure_parquet))

        hdfs_results_dir = _hdfs_results_dir(cfg, spark=spark, category="q6")
        params = _require_personalization(args)
        _save_personalization(cfg, spark=spark, params=params, hdfs_results_dir=hdfs_results_dir)

        if args.fmt == "csv":
            trips = load_trip_2024_csv(spark, path=cfg.datasets.trip_2024_csv)
            zones = load_zones_csv(spark, path=cfg.datasets.zones_csv)
        else:
            trips = load_trip_2024_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "trip_2024"))
            zones = load_zones_parquet(spark, path=parquet_dataset_path(cfg.outputs.parquet_base, "zones"))

        df = q6_dataframe(trips, zones, params)
        name = f"Q6_DF_{args.fmt}"

        elapsed = time_action(lambda: df.count())

        _save_explain(cfg, name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _save_output_df(cfg, category="q6", name=name, df=df, hdfs_results_dir=hdfs_results_dir)
        _write_timing_artifact(cfg, spark=spark, category="q6", name=name, elapsed_seconds=elapsed, hdfs_results_dir=hdfs_results_dir)

    finally:
        spark.stop()


def cmd_q6_scaling(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)
    params = _require_personalization(args)

    hdfs_results_dir: str | None = None
    if cfg.outputs.results_base_hdfs:
        group_id = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        hdfs_results_dir = coalesce_output_path(cfg.outputs.results_base_hdfs, "q6-scaling", group_id)

    presets = {
        "2exec_x4cores_8g": {
            "spark.executor.instances": "2",
            "spark.executor.cores": "4",
            "spark.executor.memory": "8g",
        },
        "4exec_x2cores_4g": {
            "spark.executor.instances": "4",
            "spark.executor.cores": "2",
            "spark.executor.memory": "4g",
        },
        "8exec_x1core_2g": {
            "spark.executor.instances": "8",
            "spark.executor.cores": "1",
            "spark.executor.memory": "2g",
        },
    }

    timings: list[TimingRecord] = []

    trip2024_parquet = parquet_dataset_path(cfg.outputs.parquet_base, "trip_2024")
    zones_parquet = parquet_dataset_path(cfg.outputs.parquet_base, "zones")

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    single = getattr(args, "single_config", None)
    if single:
        presets = {single: presets[single]}

    for label, extra_conf in presets.items():
        spark = build_spark_session(
            app_name=f"Q6_scaling_{label}",
            master=master,
            timezone=cfg.spark_timezone,
            shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
            eventlog_dir=cfg.outputs.eventlog_dir,
            extra_conf=extra_conf,
        )

        try:
            if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
                ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

            _save_personalization(cfg, spark=spark, params=params, hdfs_results_dir=hdfs_results_dir)

            ds = load_all(
                spark,
                trip_2015_path=parquet_dataset_path(cfg.outputs.parquet_base, "trip_2015"),
                trip_2024_path=trip2024_parquet,
                zones_path=zones_parquet,
                fmt="parquet",
            )

            df = q6_dataframe(ds.trip_2024, ds.zones, params)
            elapsed = time_action(lambda: df.count())
            timings.append(
                TimingRecord(
                    label=label,
                    query_id="Q6_SCALING",
                    api="DATAFRAME",
                    input_format="parquet",
                    elapsed_seconds=float(elapsed),
                    run_index=0,
                )
            )

            _save_explain(cfg, name=f"Q6_scaling_{label}", df=df, hdfs_results_dir=hdfs_results_dir)

            timings_path = cfg.outputs.results_dir_local / "q6_scaling_timings.json"
            save_timings_json(timings_path, timings)

            summary_path = cfg.outputs.results_dir_local / "q6_scaling_summary.txt"
            save_summary_txt(summary_path, timings)

            if hdfs_results_dir:
                write_text(spark, path=coalesce_output_path(hdfs_results_dir, "q6_scaling_timings.json"), text=timings_path.read_text(encoding="utf-8"))
                write_text(spark, path=coalesce_output_path(hdfs_results_dir, "q6_scaling_summary.txt"), text=summary_path.read_text(encoding="utf-8"))

        finally:
            spark.stop()

    timings_path = cfg.outputs.results_dir_local / "q6_scaling_timings.json"
    save_timings_json(timings_path, timings)
    save_summary_txt(cfg.outputs.results_dir_local / "q6_scaling_summary.txt", timings)


def cmd_join_study(args: argparse.Namespace) -> None:
    cfg = _build_config(args)
    _ensure_local_results_dirs(cfg)

    master = "local[*]" if cfg.storage == StorageBackend.LOCAL else None

    spark = build_spark_session(
        app_name="join_study",
        master=master,
        timezone=cfg.spark_timezone,
        shuffle_partitions=cfg.spark_shuffle_partitions_local if cfg.storage == StorageBackend.LOCAL else None,
        eventlog_dir=cfg.outputs.eventlog_dir,
    )

    try:
        if cfg.outputs.eventlog_dir and cfg.outputs.eventlog_dir.startswith("hdfs://"):
            ensure_hdfs_dir(spark, path=cfg.outputs.eventlog_dir)

        _maybe_convert_all(cfg, spark=spark, enabled=bool(args.ensure_parquet))

        params = _require_personalization(args)

        output_base_dir = str(cfg.outputs.results_dir_local)
        if cfg.outputs.results_base_hdfs:
            group_id = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output_base_dir = coalesce_output_path(cfg.outputs.results_base_hdfs, "join-study", group_id)

        write_text(spark, path=coalesce_output_path(output_base_dir, "personalization.txt"), text=_personalization_payload(params))

        run_zone_join_study_two_runs(
            spark,
            trips_2024_parquet_path=parquet_dataset_path(cfg.outputs.parquet_base, "trip_2024"),
            zones_parquet_path=parquet_dataset_path(cfg.outputs.parquet_base, "zones"),
            params=params,
            output_base_dir=output_base_dir,
            zones_limit=int(args.limit),
        )

    finally:
        spark.stop()


def cmd_plot_timings(args: argparse.Namespace) -> None:
    plot_timings_per_query(args.timings_json, args.out_dir)


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Big Data Project – PySpark")

    sub = p.add_subparsers(dest="cmd", required=True)

    def add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--storage", choices=["local", "hdfs"], default="local")
        sp.add_argument("--hdfs-base", default="hdfs://hdfs-namenode:9000")
        sp.add_argument("--hdfs-user", default=None)
        sp.add_argument("--parquet-base", default=None)
        sp.add_argument("--eventlog-dir", default=None)
        sp.add_argument("--hdfs-results-base", dest="hdfs_results_base", default=None)
        sp.add_argument("--results-dir", default="results")
        sp.add_argument("--am", type=int, default=None)

    p_convert = sub.add_parser("convert", help="Μετατροπή CSV -> Parquet")
    add_common(p_convert)
    p_convert.set_defaults(func=cmd_convert)

    p_run_all = sub.add_parser("run-all", help="Εκτέλεση όλων των ζητούμενων υλοποιήσεων + timings")
    add_common(p_run_all)
    p_run_all.add_argument("--skip-convert", action="store_true")
    p_run_all.add_argument("--repeats", type=int, default=1)
    p_run_all.add_argument("--warmup", action="store_true")
    p_run_all.add_argument("--q6-input-format", choices=["csv", "parquet"], default="parquet")
    p_run_all.set_defaults(func=cmd_run_all)

    p_q1 = sub.add_parser("q1", help="Q1 single-run (RDD/DF/UDF)")
    add_common(p_q1)
    p_q1.add_argument("--api", choices=["rdd", "df", "df-udf"], default="df")
    p_q1.add_argument("--fmt", choices=["csv", "parquet"], default="csv")
    p_q1.add_argument("--ensure-parquet", action="store_true")
    p_q1.set_defaults(func=cmd_q1)

    p_q2 = sub.add_parser("q2", help="Q2 single-run (RDD/DF/SQL)")
    add_common(p_q2)
    p_q2.add_argument("--api", choices=["rdd", "df", "sql"], default="sql")
    p_q2.add_argument("--fmt", choices=["csv", "parquet"], default="csv")
    p_q2.add_argument("--ensure-parquet", action="store_true")
    p_q2.set_defaults(func=cmd_q2)

    p_q3 = sub.add_parser("q3", help="Q3 single-run (DF/SQL)")
    add_common(p_q3)
    p_q3.add_argument("--api", choices=["df", "sql"], default="sql")
    p_q3.add_argument("--fmt", choices=["csv", "parquet"], default="parquet")
    p_q3.add_argument("--ensure-parquet", action="store_true")
    p_q3.set_defaults(func=cmd_q3)

    p_q4 = sub.add_parser("q4", help="Q4 single-run (SQL)")
    add_common(p_q4)
    p_q4.add_argument("--fmt", choices=["csv", "parquet"], default="parquet")
    p_q4.add_argument("--ensure-parquet", action="store_true")
    p_q4.set_defaults(func=cmd_q4)

    p_q5 = sub.add_parser("q5", help="Q5 single-run (DF)")
    add_common(p_q5)
    p_q5.add_argument("--fmt", choices=["csv", "parquet"], default="parquet")
    p_q5.add_argument("--limit", type=int, default=None)
    p_q5.add_argument("--ensure-parquet", action="store_true")
    p_q5.set_defaults(func=cmd_q5)

    p_q6 = sub.add_parser("q6", help="Q6 single-run (DF)")
    add_common(p_q6)
    p_q6.add_argument("--fmt", choices=["csv", "parquet"], default="parquet")
    p_q6.add_argument("--ensure-parquet", action="store_true")
    p_q6.set_defaults(func=cmd_q6)

    p_q6s = sub.add_parser("q6-scaling", help="Q6 scaling: 2x4, 4x2, 8x1 (όπως ζητά η εκφώνηση)")
    add_common(p_q6s)
    p_q6s.add_argument("--single-config", choices=["2exec_x4cores_8g", "4exec_x2cores_4g", "8exec_x1core_2g"], default=None)
    p_q6s.set_defaults(func=cmd_q6_scaling)

    p_join = sub.add_parser("join-study", help="Μέρος 1Β: join optimizer study")
    add_common(p_join)
    p_join.add_argument("--limit", type=int, default=50)
    p_join.add_argument("--ensure-parquet", action="store_true")
    p_join.set_defaults(func=cmd_join_study)

    p_plot = sub.add_parser("plot-timings", help="Δημιουργία PNG διαγραμμάτων από timings.json")
    p_plot.add_argument("--timings-json", default="results/timings.json")
    p_plot.add_argument("--out-dir", default="results/plots")
    p_plot.set_defaults(func=cmd_plot_timings)

    return p


def main(argv: list[str] | None = None) -> None:
    parser = _parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
