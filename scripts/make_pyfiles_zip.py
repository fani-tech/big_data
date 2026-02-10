from __future__ import annotations

import argparse
import zipfile
from pathlib import Path


def _should_skip(path: Path) -> bool:
    parts = set(path.parts)
    if "__pycache__" in parts:
        return True
    if path.suffix == ".pyc":
        return True
    return False


def build_pyfiles_zip(*, project_root: Path, out_path: Path) -> Path:
    pkg_dir = project_root / "src" / "bigdata_project"
    if not pkg_dir.exists():
        raise FileNotFoundError(f"Package dir not found: {pkg_dir}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in pkg_dir.rglob("*"):
            if p.is_dir() or _should_skip(p):
                continue

            rel = p.relative_to(pkg_dir)
            arcname = str(Path("bigdata_project") / rel)
            zf.write(p, arcname)

    return out_path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build --py-files zip for Spark (bigdata_project)")
    parser.add_argument(
        "--out",
        default="dist/bigdata_project_pyfiles.zip",
        help="Output zip path (default: dist/bigdata_project_pyfiles.zip)",
    )
    args = parser.parse_args(argv)

    project_root = Path(__file__).resolve().parents[1]
    out_path = Path(args.out)

    built = build_pyfiles_zip(project_root=project_root, out_path=out_path)
    print(str(built))


if __name__ == "__main__":
    main()
