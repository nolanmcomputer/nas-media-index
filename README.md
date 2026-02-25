# NAS Media Indexing & Search Service

A PostgreSQL-backed media catalog that indexes multi-terabyte NAS storage and exposes searchable REST endpoints over a local network.

## Features
- Incremental filesystem scanning with idempotent upserts
- PostgreSQL schema optimized for search and analytics
- FastAPI-based REST API with OpenAPI documentation
- Duplicate detection (hash-ready)
- Designed for long-running operation on Raspberry Pi hardware

## Architecture
- Python ingestion service (`scan.py`)
- PostgreSQL database
- FastAPI application (`api.py`)
- Environment-based configuration via `.env`

## Setup

### Prerequisites
- Python 3.11+
- PostgreSQL 14+
- Linux (tested on Raspberry Pi OS)

### Install
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
