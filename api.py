from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
import mimetypes
import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, Query, HTTPException
from fastapi import Header, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "web" / "templates"))

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing in .env")

app = FastAPI(title="NAS Catalog API", version="0.1.0")

def parse_range(range_header: str, file_size: int):
    """
    Parse a single HTTP Range header.
    Supports:
      - bytes=start-end
      - bytes=start-
      - bytes=-suffix
    Returns (start, end) inclusive, or None if invalid/unsupported.
    """
    if not range_header or not range_header.startswith("bytes="):
        return None

    spec = range_header[len("bytes="):].strip()
    if "," in spec:
        # multiple ranges not supported
        return None

    start_s, end_s = spec.split("-", 1)

    try:
        if start_s == "":
            # suffix range last N bytes
            suffix = int(end_s)
            if suffix <= 0:
                return None
            start = max(file_size - suffix, 0)
            end = file_size - 1
            return start, end

        start = int(start_s)
        if end_s == "":
            end = file_size - 1
        else:
            end = int(end_s)
    except ValueError:
        return None

    if start < 0 or end < start:
        return None
    if start >= file_size:
        return None

    end = min(end, file_size - 1)
    return start, end

def iter_file_range(path: Path, start: int, end: int, chunk_size: int = 1024 * 1024):
    """Yield bytes from file [start, end] inclusive."""
    with path.open("rb") as f:
        f.seek(start)
        remaining = end - start + 1
        while remaining > 0:
            chunk = f.read(min(chunk_size, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk

def get_conn():
    # Simple connection-per-request
    # TODO add pooling (psycopg_pool) for scalability
    return psycopg.connect(DATABASE_URL)

@app.get("/health")
def health():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            return {"ok": True, "db": cur.fetchone()[0]}


@app.get("/files")
def search_files(
    request: Request,
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
    # ignore non-movie files
    where.append("abs_path NOT ILIKE %s")
    params.append("%jpg")

    where.append("abs_path NOT ILIKE %s")
    params.append("%.jpeg")

    where.append("abs_path NOT ILIKE %s")
    params.append("%.png")

    where.append("abs_path NOT ILIKE %s")
    params.append("%.txt")

    where.append("abs_path NOT ILIKE %s")
    params.append("%.md")

    where.append("abs_path NOT ILIKE %s")
    params.append("%.torrent")

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT id, abs_path, size_bytes, mtime, sha256
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

    base = str(request.base_url).rstrip("/") # http://XXX.XXX.X.XX:8000

    return [
        {
	    "id": int(r[0]),
            "abs_path": r[1],
            "size_bytes": int(r[2]),
            "mtime": r[3].isoformat(),
            "sha256": r[4],
	    "http_url": f"{base}/media/{r[0]}",
	    "vlc_url": f"{base}/media/{r[0]}",
        }
        for r in rows
    ]

@app.get("/media/{file_id}")
def media(file_id: int, range: str | None = Header(default=None)):
    # Look up file path by ID
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT abs_path FROM files WHERE id = %s", (file_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Not found")
            abs_path = row[0]

    path = Path(abs_path)

    # Verification
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="File missing on disk")

    # Proper root?
    allowed_root = os.environ.get("SCAN_ROOT")
    if allowed_root:
        allowed = Path(allowed_root).resolve()
        try:
            resolved = path.resolve()
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="File missing on disk")

        if resolved != allowed and allowed not in resolved.parents:
            raise HTTPException(status_code=403, detail="Path not allowed")

    file_size = path.stat().st_size
    content_type, _ = mimetypes.guess_type(str(path))
    content_type = content_type or "application/octet-stream"

    # No Range header = stream full file (200)
    if not range:
        headers = {
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
        }
        return StreamingResponse(
            iter_file_range(path, 0, file_size - 1),
            media_type=content_type,
            headers=headers,
        )

    # Range request = 206 Partial Content
    byte_range = parse_range(range, file_size)
    if byte_range is None:
        # Invalid or unsupported range
        return Response(status_code=416, headers={"Content-Range": f"bytes */{file_size}"})

    start, end = byte_range
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(end - start + 1),
    }

    return StreamingResponse(
        iter_file_range(path, start, end),
        status_code=206,
        media_type=content_type,
        headers=headers,
    )

# Homepage
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
	return templates.TemplateResponse("index.html", {"request": request})

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
    # TODO populate sha256
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
