# AGENTS.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Commands

Install dependencies:
```
pip install -r requirements.txt
```

Run the web console (development):
```
python app.py
```
Accessible at http://localhost:9090.

Run the migration script directly (without the web UI):
```
# Dry-run (no writes)
python migrate_nl2sql.py --dry-run

# Write to local Docker MatrixOne instance
python migrate_nl2sql.py --target local

# Write to remote workspace
python migrate_nl2sql.py --target remote
```

## Architecture

This project is a **Flask web console** for managing and migrating NL2SQL knowledge bases stored in a MatrixOne (MySQL-compatible) database.

### Two databases involved

- **Old workspace** (`OLD_REMOTE` in `migrate_nl2sql.py`): The source of data for one-time migration. Its `nl2sql_knowledge` table uses a different schema (`type`/`key`/`value`/`meta` columns).
- **New workspace** (`moi` database): The target. Used by both `migrate_nl2sql.py` and the live app (`db_service.py`). Has two core tables: `knowledge_base` and `nl2sql_knowledge` (with `knowledge_type`/`knowledge_key`/`knowledge_value`/`associate_tables` columns).
- Two targets are supported everywhere via a `target` parameter: `local` (Docker, port 16001) and `remote` (MatrixOne cloud).

### Layer separation

- `app.py` — Flask routes only; never writes SQL directly. All DB access is delegated to `db_service.py`.
- `db_service.py` — All SQL logic. Each function opens and closes its own `pymysql` connection. Returns `{"ok": True/False, ...}` dicts that routes pass through as JSON.
- `migrate_nl2sql.py` — Standalone one-time migration script. Contains DDL for both tables, schema transformation logic (old → new column names, old `tables` JSON format → new format), and a `TABLE_ID_MAP` that translates old catalog table IDs to new ones. The same `TABLE_ID_MAP` is duplicated in `db_service.py` for live use.
- `templates/` — Jinja2 templates rendered by Flask. `index.html` is the migration dashboard; `kb_list.html` and `kb_detail.html` are knowledge base management views.

### Migration task flow

The web console spawns `migrate_nl2sql.py` as a subprocess (`/api/run`), streams stdout line-by-line into a shared `task_state` dict protected by a `threading.Lock`, and exposes `/api/status?since=N` for incremental log polling from the frontend.

### `tables` JSON field format

The `knowledge_base.tables` column stores a JSON array. The new format expected by the app is:
```json
[{"db_name": "jst_flat_table", "table_ids": [40169, 40148], "table_names": ["revenue_cost", "bpc_consolidated_report"], "parents": ["catalog-10001", "database-1"]}]
```
`db_service.build_tables_json()` constructs this from a list of table names. The migration script's `convert_tables_json()` transforms the old format into this shape.

### `.kiro/steering/`

Contains Kiro AI agent steering rules for memory management (Memoria MCP integration). These are not relevant to the Flask application itself — they govern how the Kiro AI assistant manages persistent memory across sessions.

## Deployment

`vercel.json` is present but this app uses long-running threads and a mutable global `task_state`; it is not stateless and will not work correctly on Vercel's serverless runtime. The intended deployment is as a persistent process (e.g., `python app.py`).
