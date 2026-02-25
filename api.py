from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing in .env")

app = FastAPI(title="NAS Catalog API", version="0.1.0")


def get_conn():
    # Simple connection-per-request. Fine for home use.
    # Later you can add pooling (psycopg_pool) for scalability.
    return psycopg.connect(DATABASE_URL)


@app.get("/health")
def health():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            return {"ok": True, "db": cur.fetchone()[0]}


@app.get("/files")
def search_files(
    q: Optional[str] = Query(default=None, description="Search substring in path"),
    root: Optional[str] = Query(default=None, description="Filter by root name (e.g. FilmsRoot)"),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    where = []
    params = []

    if q:
        where.append("abs_path ILIKE %s")
        params.append(f"%{q}%")

    if root:
        where.append("root = %s")
        params.append(root)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT abs_path, size_bytes, mtime, sha256
        FROM files
        {where_sql}
        ORDER BY mtime DESC
        LIMIT %s OFFSET %s
    """
    params.extend([limit, offset])

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    return [
        {
            "abs_path": r[0],
            "size_bytes": int(r[1]),
            "mtime": r[2].isoformat(),
            "sha256": r[3],
        }
        for r in rows
    ]


@app.get("/stats")
def stats():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM files;")
            total_files = cur.fetchone()[0]

            cur.execute("SELECT COALESCE(SUM(size_bytes), 0) FROM files;")
            total_bytes = cur.fetchone()[0]

            cur.execute("""
                SELECT root, COUNT(*) AS n, COALESCE(SUM(size_bytes), 0) AS bytes
                FROM files
                GROUP BY root
                ORDER BY bytes DESC
            """)
            by_root = [{"root": r[0], "files": int(r[1]), "bytes": int(r[2])} for r in cur.fetchall()]

    return {"total_files": int(total_files), "total_bytes": int(total_bytes), "by_root": by_root}


@app.get("/duplicates")
def duplicates(limit: int = Query(default=100, ge=1, le=500)):
    # Only works once you populate sha256, but endpoint is fine to have now.
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sha256, COUNT(*) AS copies, SUM(size_bytes) AS total_bytes,
                       ARRAY_AGG(abs_path ORDER BY abs_path) AS paths
                FROM files
                WHERE sha256 IS NOT NULL
                GROUP BY sha256
                HAVING COUNT(*) > 1
                ORDER BY total_bytes DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    return [
        {
            "sha256": r[0],
            "copies": int(r[1]),
            "total_bytes": int(r[2]),
            "paths": r[3],
        }
        for r in rows
    ]
