"""
Helper script: download the RetailRocket dataset from Kaggle via kagglehub
and copy events.csv into ./data/events.csv so the pipeline can use it.

Run once before `docker compose up`:

    pip install kagglehub
    python download_dataset.py

Requires you to be logged in to Kaggle (kagglehub will prompt for your
Kaggle credentials / token on first use).
"""

import os
import shutil
import sys


def main() -> None:
    try:
        import kagglehub
    except ImportError:
        print(
            "kagglehub is not installed. Install it first:\n"
            "    pip install kagglehub",
            file=sys.stderr,
        )
        sys.exit(1)

    print("[download] Fetching retailrocket/ecommerce-dataset from Kaggle...")
    path = kagglehub.dataset_download("retailrocket/ecommerce-dataset")
    print(f"[download] kagglehub downloaded files to: {path}")

    # Find events.csv inside the downloaded folder (it sits at the top level)
    src = None
    for root, _dirs, files in os.walk(path):
        for fn in files:
            if fn.lower() == "events.csv":
                src = os.path.join(root, fn)
                break
        if src:
            break

    if not src:
        print(
            f"[download] ERROR: could not find events.csv inside {path}",
            file=sys.stderr,
        )
        sys.exit(2)

    project_root = os.path.dirname(os.path.abspath(__file__))
    dst = os.path.join(project_root, "data", "events.csv")
    os.makedirs(os.path.dirname(dst), exist_ok=True)

    print(f"[download] Copying {src} -> {dst}")
    shutil.copyfile(src, dst)

    size_mb = os.path.getsize(dst) / (1024 * 1024)
    print(f"[download] Done. events.csv is now at {dst} ({size_mb:.1f} MB).")
    print("[download] You can now run: docker compose up -d --build")


if __name__ == "__main__":
    main()
