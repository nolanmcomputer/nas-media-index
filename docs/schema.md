# Database Schema

This document describes the PostgreSQL schema used by the **NAS Media Indexing & Search Service**.  
The schema is designed to support incremental filesystem scans, search queries, and storage analytics on large NAS volumes.

---

## Overview

The database models two core concepts:

1. **Scan runs** — each execution of the filesystem scanner
2. **Files** — individual files discovered during scans

This separation allows the system to:
- Track when scans occurred
- Detect new, changed, or missing files
- Perform historical analysis without duplicating data

---

## Tables

### `scan_runs`

Represents a single execution of the scanner over a filesystem root.

Each scan records summary information that can be used for monitoring, debugging, and reporting.

#### Columns

| Column          | Type        | Description |
|-----------------|-------------|-------------|
| `id`            | `BIGINT`    | Primary key |
| `started_at`    | `TIMESTAMPTZ` | Timestamp when the scan began |
| `finished_at`   | `TIMESTAMPTZ` | Timestamp when the scan completed |
| `root`          | `TEXT`      | Logical name of the scanned root (e.g. `FilmsRoot`) |
| `files_seen`    | `BIGINT`    | Total number of files encountered |
| `files_changed` | `BIGINT`    | Number of new or modified files |

#### Design notes
- `TIMESTAMPTZ` is used to ensure timestamps are timezone-aware.
- Summary counts allow quick inspection of scan activity without expensive queries.
- Storing the logical root name (rather than a mount path) decouples the schema from filesystem UUID changes.

---

### `files`

Stores metadata for every indexed file on the NAS.

Each row represents the *latest known state* of a file.

#### Columns

| Column            | Type        | Description |
|-------------------|-------------|-------------|
| `id`              | `BIGINT`    | Primary key |
| `root`            | `TEXT`      | Logical root name the file belongs to |
| `rel_path`        | `TEXT`      | Path relative to the scanned root |
| `abs_path`        | `TEXT`      | Absolute filesystem path (unique) |
| `size_bytes`      | `BIGINT`    | File size in bytes |
| `mtime`           | `TIMESTAMPTZ` | Last modification time |
| `sha256`          | `TEXT`      | Optional content hash (used for duplicates) |
| `last_seen_run_id`| `BIGINT`    | Foreign key to `scan_runs(id)` |

#### Design notes
- `abs_path` is marked `UNIQUE` to prevent duplicate records and enable upserts.
- `last_seen_run_id` enables detection of files that disappeared since the last scan.
- `sha256` is nullable to allow fast scans without hashing; hashes can be computed incrementally.

---

## Indexes

The schema includes several indexes to optimize common queries:

### `idx_files_sha256`
Optimizes duplicate detection queries that group by file content hash.

```sql
SELECT sha256, COUNT(*)
FROM files
GROUP BY sha256
HAVING COUNT(*) > 1;
