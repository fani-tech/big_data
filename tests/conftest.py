from __future__ import annotations

import sys
import tempfile
import zipfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


# Εξασφάλιση ότι το src/ είναι στο import path (χρήσιμο αν δεν έχει γίνει pip install -e .)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def _build_pyfiles_zip() -> str:
    """Δημιουργεί zip με το package bigdata_project ώστε να είναι importable από Spark workers.

    Χωρίς αυτό, τα RDD tests μπορεί να αποτύχουν με:
      ModuleNotFoundError: No module named 'bigdata_project'
    """

    pkg_dir = SRC_DIR / "bigdata_project"
    out_path = Path(tempfile.gettempdir()) / "bigdata_project_pyfiles_tests.zip"

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in pkg_dir.rglob("*"):
            if p.is_dir():
                continue
            if "__pycache__" in p.parts or p.suffix == ".pyc":
                continue

            rel = p.relative_to(pkg_dir)
            arcname = str(Path("bigdata_project") / rel)
            zf.write(p, arcname)

    return str(out_path)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("bigdata_project_tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Διανομή του local package στους workers
    spark.sparkContext.addPyFile(_build_pyfiles_zip())
    yield spark
    spark.stop()
