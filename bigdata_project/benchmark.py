from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable, Optional


@dataclass(frozen=True)
class TimingRecord:
    label: str
    query_id: str
    api: str
    input_format: str
    elapsed_seconds: float
    run_index: int


def time_action(action: Callable[[], None]) -> float:
    start = time.perf_counter()
    action()
    end = time.perf_counter()
    return end - start


def run_repeated(label: str, query_id: str, api: str, input_format: str, action: Callable[[], None], *, repeats: int) -> list[TimingRecord]:
    records: list[TimingRecord] = []
    for i in range(repeats):
        elapsed = time_action(action)
        records.append(
            TimingRecord(
                label=label,
                query_id=query_id,
                api=api,
                input_format=input_format,
                elapsed_seconds=float(elapsed),
                run_index=i,
            )
        )
    return records


def save_timings_json(path: str | Path, timings: Iterable[TimingRecord]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    data = [asdict(t) for t in timings]
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_timings_json(path: str | Path) -> list[TimingRecord]:
    p = Path(path)
    with open(p, "r", encoding="utf-8") as f:
        raw = json.load(f)

    return [TimingRecord(**x) for x in raw]


def plot_timings(path_in: str | Path, path_out: str | Path) -> None:
    import matplotlib

    matplotlib.use("Agg")

    import matplotlib.pyplot as plt

    records = load_timings_json(path_in)

    by_query: dict[str, list[TimingRecord]] = {}
    for r in records:
        by_query.setdefault(r.query_id, []).append(r)

    fig, axes = plt.subplots(len(by_query), 1, figsize=(10, 4 * max(1, len(by_query))))
    if len(by_query) == 1:
        axes = [axes]

    for ax, (qid, rs) in zip(axes, sorted(by_query.items(), key=lambda x: x[0])):
        by_label: dict[str, list[float]] = {}
        for r in rs:
            by_label.setdefault(r.label, []).append(r.elapsed_seconds)

        labels = list(by_label.keys())
        means = [sum(v) / len(v) for v in by_label.values()]

        ax.bar(labels, means)
        ax.set_title(f"{qid} – μέσος χρόνος (δευτερόλεπτα)")
        ax.set_ylabel("seconds")
        ax.tick_params(axis="x", rotation=30)

    fig.tight_layout()
    Path(path_out).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path_out)
    plt.close(fig)


def plot_timings_per_query(path_in: str | Path, out_dir: str | Path) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")

    import matplotlib.pyplot as plt

    records = load_timings_json(path_in)

    by_query: dict[str, list[TimingRecord]] = {}
    for r in records:
        by_query.setdefault(r.query_id, []).append(r)

    out_paths: list[Path] = []
    out_dir_p = Path(out_dir)
    out_dir_p.mkdir(parents=True, exist_ok=True)

    for qid, rs in sorted(by_query.items(), key=lambda x: x[0]):
        by_label: dict[str, list[float]] = {}
        for r in rs:
            by_label.setdefault(r.label, []).append(r.elapsed_seconds)

        labels = sorted(by_label.keys())
        means = [sum(by_label[l]) / len(by_label[l]) for l in labels]

        fig, ax = plt.subplots(1, 1, figsize=(10, 4))
        ax.bar(labels, means)
        ax.set_title(f"{qid} – μέσος χρόνος (δευτερόλεπτα)")
        ax.set_ylabel("seconds")
        ax.tick_params(axis="x", rotation=30)
        fig.tight_layout()

        out_path = out_dir_p / f"{qid}_timings.png"
        fig.savefig(out_path)
        plt.close(fig)

        out_paths.append(out_path)

    return out_paths


def maybe_warmup(action: Callable[[], None], enabled: bool) -> None:
    if not enabled:
        return
    action()


def format_stats(values: list[float]) -> str:
    if not values:
        return ""
    v = sorted(values)
    return f"min={v[0]:.4f}s avg={sum(v)/len(v):.4f}s max={v[-1]:.4f}s"


def summarize_timings(records: list[TimingRecord]) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, list[float]]] = {}

    for r in records:
        out.setdefault(r.query_id, {}).setdefault(r.label, []).append(r.elapsed_seconds)

    return {
        qid: {label: format_stats(vals) for label, vals in labels.items()}
        for qid, labels in out.items()
    }


def save_summary_txt(path: str | Path, records: list[TimingRecord]) -> None:
    summary = summarize_timings(records)
    lines: list[str] = []
    for qid in sorted(summary.keys()):
        lines.append(f"{qid}")
        for label in sorted(summary[qid].keys()):
            lines.append(f"  - {label}: {summary[qid][label]}")
        lines.append("")

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("\n".join(lines), encoding="utf-8")
