# NAS Media Indexing & Search Service

A self-hosted media indexing and search service for NAS storage.

The filesystem remains the source of truth. A Python scanner indexes file
metadata into PostgreSQL, while a FastAPI application exposes search,
statistics, duplicate detection, and HTTP media streaming endpoints.

The application is deployed with Docker on a Raspberry Pi and is designed for
long-running unattended operation.

## Features

- Incremental filesystem scanning with idempotent PostgreSQL upserts
- Reconciliation of deleted and renamed files after successful scans
- PostgreSQL-backed searchable media catalog
- FastAPI REST API with OpenAPI documentation
- Browser-based search interface
- HTTP media streaming with byte-range support
- Pagination for large media collections
- Duplicate detection support using stored file hashes
- Environment-based configuration
- Dockerized application and database services
- Automated nightly scans using a systemd timer
- Designed for persistent operation on Raspberry Pi / Linux hardware

## Architecture

```text
                         NAS filesystem
                       (source of truth)
                              |
                              | bind mount
                              v
                     +------------------+
                     |     nas-api      |
                     |------------------|
                     | FastAPI          |
                     | scan.py          |
                     | web UI           |
                     | HTTP streaming   |
                     +---------+--------+
                               |
                               | PostgreSQL
                               v
                     +------------------+
                     |   nas-postgres   |
                     |  PostgreSQL 16   |
                     +------------------+

                              ^
                              |
                   docker exec /app/scan.py
                              |
                     +------------------+
                     | systemd timer    |
                     | nightly scan     |
                     +------------------+
```

## Components

- **`scan.py`** Recursively scans the configured media root and synchronizes filesystem metadata with PostgreSQL
- **`api.py`** FastAPI application providing search, statistics, duplicate detection, health checks, and media streaming
- **PostgreSQL** Stores indexed filesystem metadata and scan history
- **Web UI** Lightweight browser interface for searching the media catalog and opening media streams
- **Docker Compose** Runs the API/scanner container and PostgreSQL database
- **systemd timer** Periodically executes the scanner inside the running API container

## Data Model

The primary database tables are:

### `files`

Stores the current indexed state of the filesystem.

Fields include:

- `id`
- `root`
- `rel_path`
- `abs_path`
- `size_bytes`
- `mtime`
- `sha256`
- `last_seen_run_id`

`abs_path` is unique and is used for idempotent scanner upserts

### `scan_runs`

Records individual scanner executions.

Fields include:

- `id`
- `started_at`
- `finished_at`
- `root`
- `files_seen`
- `files_changed`

## Scanner Behaviour

The scanner treats the NAS filesystem as the authoritative source.

For every scan:

1. A new `scan_runs` record is created
2. The configured filesystem root is recursively traversed
3. Hidden files and directories are ignored
4. Existing files are compared using path, size, and modification time
5. New or changed files are inserted or updated
6. Each discovered file is associated with the current scan ID
7. After a successful full scan, database entries that were not encountered are removed
8. The scan record is marked complete

This reconciliation model also handles file renames naturally:

```text
old path -> no longer observed -> removed
new path -> newly observed      -> inserted
```

The scanner can safely be run repeatedly without creating duplicate records.

## Docker Deployment

The current deployment uses two containers:

```text
nas-api
nas-postgres
```

### `nas-api`

Runs:

- FastAPI
- the browser UI
- HTTP media streaming
- 'scan.py'

The NAS filesystem is bind-mounted into the container as `/media`.

### `nas-postgres`

Runs PostgreSQL and stores the media catalog.

The API communicates with PostgreSQL using Docker's internal service network rather than `localhost`.

## Configuration

Configuration is supplied using environment variables.

Example:

```env
DATABASE_URL=postgresql://nasuser:<password>@postgres:5432/nasdb
SCAN_DATABASE_URL=postgresql://nasuser:<password>@postgres:5432/nasdb

SCAN_ROOT=/media
ROOT_NAME=FilmsRoot

API_HOST=0.0.0.0
API_PORT=8000
```

The real `.env` file is intentionally excluded from version control. See `.env.example` for a template.

## Starting the Stack

From the Compose directory:

```bash
docker compose up -d
```

Check container state:

```bash
docker compose ps
```

View API logs:

```bash
docker logs nas-api
```

View PostgreSQL logs:

```bash
docker logs nas-postgres
```

## Health Check

Test the API:

```bash
curl http://localhost:8000/health
```

Example response:

```bash
{
  "status": "ok"
}
```

FastAPI's interactive OpenAPI documentation is available at:

```text
http://<server-ip>:8000/docs
```

## Running a Scan Manually

The scanner runs inside the `nas-api` container:

```bash
docker exec -it nas-api python /app/scan.py
```

A completed scan reports information similar to:

```text
Done.
Run ID: 42
Root name: FilmsRoot
Scan path: /media
Files seen: 12500
Files new/changed: 8
Files removed: 2
Elapsed: 18.4s
```

Before running a scan, `/media` must point to the real NAS filesystem.

Because completed scans prune database entries that are no longer present, running the scanner against an incorrect or empty mount should be avoided.

## Automated Scanning

Production scans are scheduled using systemd.

Example service:

```env
[Unit]
Description=NAS Media Index filesystem scan
After=docker.service network-online.target
Requires=docker.service
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/bin/docker exec nas-api python /app/scan.py
```

Example timer:

```env
[Unit]
Description=Run NAS Media Index scan nightly

[Timer]
OnCalendar=*-*-* 03:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable the timer with:

```bash
sudo systemctl enable --now nas-media-scan.timer
```

Check it's schedule:

```bash
systemctl list-timers --all | grep nas-media
```

Scanner output can be inspected with:

```bash
journalctl -u nas-media-scan.service
```

## REST API

### Health

```http
GET /health
```

Checks API and PostgreSQL connectivity.

### Search Files

```http
GET /files
```
Opertional query parameters include:

```text
q
root
limit
offset
```

Example:

```bash
curl "http://localhost:8000/files?q=simpson&limit=20"
```

### Statistics

```http
GET /stats
```

Returns catalog statistics.

### Duplicate Detection

```http
GET /duplicates
```

Supports identifying duplicate files when hashes are available.

### Media Streaming

```http
GET /media/{filed_id}
```

Streams the underlying media file associated with a database ID.

The endpoint supports HTTP byte-range requests:

```http
Range: bytes 1000000-1999999
```

and returns:

```http
206 Partial Contenct
```

with an appropriate `Content-Range` header.

This allows compatible browser and media players to seek within large media files without downloading the entire file first.

## Web Interface

The root endpoint:

```text
http://<server-ip>:8000/
```

provides lightweight search interface.

Search results include:

- filename/path
- file size
- modification time
- HTTP streaming link
- VLC-compatible link

The UI consumes the same /files API used by the external clients.

## Storage Safety

The API does not expose arbitrary filesystem paths directly.

Media streaming uses database IDs:

```text
/media/1234
```

instead of accepting a user-supplied filesystem path.

Before serving a file, the application verifies that the resolved path remains inside the configured media root.

Database queries are parameterized using `psycopg`.

The service is intended primarily for trusted local networks or access through a VPN rather than direct public internet exposure.

## Project Structure

```text
nasdb/
├── api.py
├── scan.py
├── Dockerfile
├── requirements.txt
├── README.md
├── .env.example
├── docs/
│   ├── architecture.md
│   └── schema.md
├── sql/
│   └── schema.sql
└── web/
    └── templates/
        └── index.html
```

The production Docker Compose configuration is maintained separately from the application source so deployment concerns remain separated from application code.

## Reliability Model

The design intentionally keeps the filesystem as the source of truth.

```text
 Filesystem
    |
    v
Scanner
    |
    v
PostgreSQL index
    |
    v
FastAPI / clients
```

PostgreSQL contains an index of filesystem state rather than the authoritative copy of the media collection.

As a result:

- the scanner can be rerun safely
- the API is restartable
- the database can be reconstructed from the filesystem
- stale database records can be reconciled automatically
- media remains usable independently of the indexing service

## Current Deployment

The service currently runs on a Raspberry Pi 5 using:

- 64-bit Lunx
- Docker Engine
- Docker Compose
- PostgreSQL 16
- FastAPI
- Python
- systemd
- ext4 NAS storage

The media filesystem is mounted by the host and bind-mounted into the application container.

The Raspberry Pi deployment also serves as a practical environment for testing the service lifecycle management, backups, migrations, logging, storage mounts, container networking, and failure recovery.

## Future Improvements

Possible future work includes:

- content hashing for all indexed files
- improved duplicate analysis
- authentication and authorization
- richer search filters
- structured application logging
- Prometheus-compatible metrics
- automated integration tests
- container health checks
- CI/CD deployment
- moving suitable stateless services into a small Raspberry Pi cluster

