from __future__ import annotations

import json
from pathlib import Path


def _read_elapsed_seconds_txt(path: Path) -> float:
    raw = path.read_text(encoding="utf-8").strip()
    key, value = raw.split("=", 1)
    if key.strip() != "elapsed_seconds":
        raise ValueError(f"Unexpected payload in {path}: {raw!r}")
    return float(value.strip())


def _load_run_all_timings(path: Path) -> dict[str, float]:
    data = json.loads(path.read_text(encoding="utf-8"))
    out: dict[str, float] = {}
    for rec in data:
        if int(rec.get("run_index", 0)) != 0:
            continue
        out[str(rec["label"])] = float(rec["elapsed_seconds"])
    return out


def _plot_bar_png(*, title: str, y_label: str, items: list[tuple[str, float]], out_png: Path) -> None:
    import matplotlib.pyplot as plt

    labels = [k for k, _ in items]
    values = [v for _, v in items]

    fig_w = max(7.5, 0.75 * len(labels))
    fig, ax = plt.subplots(figsize=(fig_w, 4.5))
    ax.bar(range(len(values)), values, color="#2A6F97")
    ax.set_title(title)
    ax.set_ylabel(y_label)
    ax.set_xticks(range(len(labels)), labels, rotation=35, ha="right")
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def _parse_kv_txt(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip() or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    plots_dir_hdfs = root / "results" / "hdfs" / "plots"
    plots_dir_legacy = root / "results" / "plots"

    run_all_timings = _load_run_all_timings(root / "results" / "hdfs" / "run-all" / "timings.json")

    q1_rdd = _read_elapsed_seconds_txt(root / "results" / "hdfs" / "q1-rdd-timing" / "Q1_RDD_csv_timing.txt")
    q2_rdd = _read_elapsed_seconds_txt(root / "results" / "hdfs" / "q2-rdd-timing" / "Q2_RDD_csv_timing.txt")
    run_all_timings["Q1_RDD_csv"] = q1_rdd
    run_all_timings["Q2_RDD_csv"] = q2_rdd

    p = _parse_kv_txt(root / "results" / "hdfs" / "run-all" / "personalization.txt")
    am = p.get("am", "?")
    title_suffix = f"(AM={am})"

    def pick(keys: list[str]) -> list[tuple[str, float]]:
        return [(k, run_all_timings[k]) for k in keys if k in run_all_timings]

    convert_kv = _parse_kv_txt(root / "results" / "hdfs" / "convert" / "conversion_time.txt")
    if "conversion_elapsed_seconds" in convert_kv:
        conv_s = float(convert_kv["conversion_elapsed_seconds"])
        _plot_bar_png(
            title=f"CSV -> Parquet conversion time {title_suffix}",
            y_label="Elapsed seconds",
            items=[("convert_to_parquet", conv_s)],
            out_png=plots_dir_hdfs / "convert_timings.png",
        )
        _plot_bar_png(
            title=f"CSV -> Parquet conversion time {title_suffix}",
            y_label="Elapsed seconds",
            items=[("convert_to_parquet", conv_s)],
            out_png=plots_dir_legacy / "CONVERT_timings.png",
        )

    _plot_bar_png(
        title=f"Q1 - Execution time by implementation {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q1_RDD_csv", "Q1_DF_csv", "Q1_DF_parquet", "Q1_DF_UDF_csv", "Q1_DF_UDF_parquet"]),
        out_png=plots_dir_hdfs / "q1_timings.png",
    )
    _plot_bar_png(
        title=f"Q1 - Execution time by implementation {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q1_RDD_csv", "Q1_DF_csv", "Q1_DF_parquet", "Q1_DF_UDF_csv", "Q1_DF_UDF_parquet"]),
        out_png=plots_dir_legacy / "Q1_timings.png",
    )

    _plot_bar_png(
        title=f"Q2 - Execution time by implementation {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q2_RDD_csv", "Q2_DF_csv", "Q2_DF_parquet", "Q2_SQL_csv", "Q2_SQL_parquet"]),
        out_png=plots_dir_hdfs / "q2_timings.png",
    )
    _plot_bar_png(
        title=f"Q2 - Execution time by implementation {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q2_RDD_csv", "Q2_DF_csv", "Q2_DF_parquet", "Q2_SQL_csv", "Q2_SQL_parquet"]),
        out_png=plots_dir_legacy / "Q2_timings.png",
    )

    _plot_bar_png(
        title=f"Q3 - Execution time by implementation {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q3_DF_csv", "Q3_DF_parquet", "Q3_SQL_csv", "Q3_SQL_parquet"]),
        out_png=plots_dir_hdfs / "q3_timings.png",
    )
    _plot_bar_png(
        title=f"Q3 - Execution time by implementation {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q3_DF_csv", "Q3_DF_parquet", "Q3_SQL_csv", "Q3_SQL_parquet"]),
        out_png=plots_dir_legacy / "Q3_timings.png",
    )

    _plot_bar_png(
        title=f"Q4 - Execution time (SQL) {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q4_SQL_csv", "Q4_SQL_parquet"]),
        out_png=plots_dir_hdfs / "q4_timings.png",
    )
    _plot_bar_png(
        title=f"Q4 - Execution time (SQL) {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q4_SQL_csv", "Q4_SQL_parquet"]),
        out_png=plots_dir_legacy / "Q4_timings.png",
    )

    _plot_bar_png(
        title=f"Q5 - Execution time (DataFrame, Parquet) {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q5_DF_default_parquet", "Q5_DF_no_broadcast_parquet"]),
        out_png=plots_dir_hdfs / "q5_timings.png",
    )
    _plot_bar_png(
        title=f"Q5 - Execution time (DataFrame, Parquet) {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q5_DF_default_parquet", "Q5_DF_no_broadcast_parquet"]),
        out_png=plots_dir_legacy / "Q5_timings.png",
    )

    _plot_bar_png(
        title=f"Q6 - Execution time (DataFrame, Parquet) {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q6_DF_parquet"]),
        out_png=plots_dir_hdfs / "q6_timings.png",
    )
    _plot_bar_png(
        title=f"Q6 - Execution time (DataFrame, Parquet) {title_suffix}",
        y_label="Elapsed seconds",
        items=pick(["Q6_DF_parquet"]),
        out_png=plots_dir_legacy / "Q6_timings.png",
    )

    scaling_paths = [
        root / "results" / "hdfs" / "q6-scale-2x4" / "q6_scaling_timings.json",
        root / "results" / "hdfs" / "q6-scale-4x2" / "q6_scaling_timings.json",
        root / "results" / "hdfs" / "q6-scale-8x1" / "q6_scaling_timings.json",
    ]
    scaling_items: list[tuple[str, float]] = []
    for p in scaling_paths:
        recs = json.loads(p.read_text(encoding="utf-8"))
        if not recs:
            continue
        rec = recs[0]
        scaling_items.append((str(rec["label"]), float(rec["elapsed_seconds"])))

    scaling_items.sort(key=lambda kv: kv[0])
    _plot_bar_png(
        title=f"Q6 - Scaling (8 cores, 16 GB total) {title_suffix}",
        y_label="Elapsed seconds",
        items=scaling_items,
        out_png=plots_dir_hdfs / "q6_scaling_timings.png",
    )
    _plot_bar_png(
        title=f"Q6 - Scaling (8 cores, 16 GB total) {title_suffix}",
        y_label="Elapsed seconds",
        items=scaling_items,
        out_png=plots_dir_legacy / "Q6_scaling_timings.png",
    )


if __name__ == "__main__":
    main()
