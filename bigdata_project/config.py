from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional


class StorageBackend(str, Enum):
    LOCAL = "local"
    HDFS = "hdfs"


class DataFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"


@dataclass(frozen=True)
class DatasetLocations:
    trip_2015_csv: str
    trip_2024_csv: str
    zones_csv: str


@dataclass(frozen=True)
class OutputLocations:
    parquet_base: str
    results_dir_local: Path
    eventlog_dir: Optional[str]
    results_base_hdfs: Optional[str]


@dataclass(frozen=True)
class AppConfig:
    storage: StorageBackend
    datasets: DatasetLocations
    outputs: OutputLocations

    spark_timezone: str = "UTC"
    spark_shuffle_partitions_local: int = 8


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_dataset_locations(storage: StorageBackend, hdfs_base: str = "hdfs://hdfs-namenode:9000") -> DatasetLocations:
    if storage == StorageBackend.LOCAL:
        return DatasetLocations(
            trip_2015_csv=str(project_root() / "data" / "sample" / "yellow_tripdata_2015.csv"),
            trip_2024_csv=str(project_root() / "data" / "sample" / "yellow_tripdata_2024.csv"),
            zones_csv=str(project_root() / "data" / "sample" / "taxi_zone_lookup.csv"),
        )

    return DatasetLocations(
        trip_2015_csv=f"{hdfs_base}/data/yellow_tripdata_2015.csv",
        trip_2024_csv=f"{hdfs_base}/data/yellow_tripdata_2024.csv",
        zones_csv=f"{hdfs_base}/data/taxi_zone_lookup.csv",
    )


def default_output_locations(
    storage: StorageBackend,
    *,
    hdfs_base: str = "hdfs://hdfs-namenode:9000",
    hdfs_user: Optional[str] = None,
    parquet_base_override: Optional[str] = None,
    eventlog_dir_override: Optional[str] = None,
    results_base_hdfs_override: Optional[str] = None,
    results_dir_local: Optional[Path] = None,
) -> OutputLocations:
    root = project_root()

    results_dir_local_final = results_dir_local or (root / "results")

    if storage == StorageBackend.LOCAL:
        parquet_base = parquet_base_override or str(results_dir_local_final / "parquet_v2")
        eventlog_dir = eventlog_dir_override or str(results_dir_local_final / "spark-events")
        return OutputLocations(
            parquet_base=parquet_base,
            results_dir_local=results_dir_local_final,
            eventlog_dir=eventlog_dir,
            results_base_hdfs=None,
        )

    if parquet_base_override:
        parquet_base = parquet_base_override
    else:
        if not hdfs_user:
            raise ValueError(
                "Λείπει --hdfs-user. Για δοκιμή χωρίς username, δώσε --parquet-base ρητά (π.χ. hdfs://.../user/<u>/data/parquet)."
            )
        parquet_base = f"{hdfs_base}/user/{hdfs_user}/data/parquet_v2"

    if eventlog_dir_override:
        eventlog_dir = eventlog_dir_override
    else:
        if not hdfs_user:
            eventlog_dir = None
        else:
            eventlog_dir = f"{hdfs_base}/user/{hdfs_user}/logs"

    if results_base_hdfs_override:
        results_base_hdfs = results_base_hdfs_override
    else:
        results_base_hdfs = f"{hdfs_base}/user/{hdfs_user}/bigdata_project" if hdfs_user else None

    return OutputLocations(
        parquet_base=parquet_base,
        results_dir_local=results_dir_local_final,
        eventlog_dir=eventlog_dir,
        results_base_hdfs=results_base_hdfs,
    )
