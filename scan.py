#!/usr/bin/env python3
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn


def dt_from_epoch(epoch_seconds: float) -> datetime:
    # store as UTC timestamptz
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


def iter_files(root: Path):
    # Walk filesystem; yields (abs_path, rel_path, size, mtime_epoch)
    for dirpath, dirnames, filenames in os.walk(root):
        # skip hidden dirs
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]

        for name in filenames:
            if name.startswith("."):
                continue
            p = Path(dirpath) / name
            try:
                st = p.stat()
            except FileNotFoundError:
                # file disappeared during scan
                continue
            except PermissionError:
                continue

            if not p.is_file():
                continue

            yield (str(p), str(p.relative_to(root)), st.st_size, st.st_mtime)


def main() -> int:
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

    db_url = os.environ.get("DATABASE_URL")
    scan_root = os.environ.get("SCAN_ROOT")
    root_name = os.environ.get("ROOT_NAME", "Root")

    if not db_url or not scan_root:
        print("Missing DATABASE_URL or SCAN_ROOT in .env", file=sys.stderr)
        return 2

    root = Path(scan_root)
    if not root.exists() or not root.is_dir():
        print(f"SCAN_ROOT is not a directory: {root}", file=sys.stderr)
        return 2

    # Connect
    conn = psycopg.connect(db_url)
    conn.execute("SET statement_timeout = '0'")  # disable timeouts for long scan
    conn.execute("SET lock_timeout = '5s'")

    files_seen = 0
    files_changed = 0

    # Create scan run row
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO scan_runs (root) VALUES (%s) RETURNING id",
            (root_name,),
        )
        run_id = cur.fetchone()[0]
    conn.commit()

    # Prepare SQL
    # lightweight check avoids counting unchanged rows as "changed":
    # 1) SELECT existing size/mtime by abs_path
    # 2) UPSERT (insert/update)
    select_existing = "SELECT size_bytes, mtime FROM files WHERE abs_path = %s"

    upsert = """
    INSERT INTO files (root, rel_path, abs_path, size_bytes, mtime, last_seen_run_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (abs_path)
    DO UPDATE SET
      root = EXCLUDED.root,
      rel_path = EXCLUDED.rel_path,
      size_bytes = EXCLUDED.size_bytes,
      mtime = EXCLUDED.mtime,
      last_seen_run_id = EXCLUDED.last_seen_run_id
    """

    start = time.time()

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("{task.completed} files"),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task(f"Scanning {root} …", total=None)

            with conn.cursor() as cur:
                for abs_path, rel_path, size, mtime_epoch in iter_files(root):
                    files_seen += 1
                    mtime_dt = dt_from_epoch(mtime_epoch)

                    # determine if changed compared to database
                    cur.execute(select_existing, (abs_path,))
                    row = cur.fetchone()
                    if row is None:
                        files_changed += 1
                    else:
                        prev_size, prev_mtime = row
                        if int(prev_size) != int(size) or prev_mtime != mtime_dt:
                            files_changed += 1

                    cur.execute(
                        upsert,
                        (root_name, rel_path, abs_path, size, mtime_dt, run_id),
                    )

                    # commit in batches for safety
                    if files_seen % 2000 == 0:
                        conn.commit()
                        progress.update(task, completed=files_seen)

                conn.commit()
                progress.update(task, completed=files_seen)

        # Finish scan run row
        elapsed = time.time() - start
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE scan_runs
                SET finished_at = now(),
                    files_seen = %s,
                    files_changed = %s
                WHERE id = %s
                """,
                (files_seen, files_changed, run_id),
            )
        conn.commit()

        print(f"\nDone.")
        print(f"Run ID: {run_id}")
        print(f"Root name: {root_name}")
        print(f"Scan path: {root}")
        print(f"Files seen: {files_seen}")
        print(f"Files new/changed: {files_changed}")
        print(f"Elapsed: {elapsed:.1f}s")
        return 0

    except KeyboardInterrupt:
        print("\nInterrupted. Committing progress…")
        conn.commit()
        return 130

    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
