# Tadawul Fast Bridge — Phase-1 (DB-free) Quickstart

This Phase-1 runs WITHOUT Postgres.
The backend validates rows using YAML-driven dynamic models, then returns `validated_rows`.
Google Apps Script is responsible for appending those validated rows into Google Sheets history tabs.

---

## 1) Required Render Env Vars
Set these in Render service environment:

- `APP_TOKEN` = your secret token (used in request header `X-APP-TOKEN`)
- `DYNAMIC_PAGES_DIR` = `config/dynamic_pages` (optional; defaults to this)

Optional:
- `APP_VERSION` (defaults if missing)
- `ENV` (defaults if missing)

---

## 2) Health (no token required)
GET `/health`

Expected:
```json
{
  "status": "ok",
  "app": "Tadawul Fast Bridge",
  "version": "5.0.0-phase1",
  "env": "production",
  "time_utc": "..."
}
