# Tadawul Fast Bridge — Phase-1 Quickstart (Python only)

## 1) Required Render Env Vars
Set these in Render service environment:

- `APP_TOKEN` = your secret token (used in request header `X-APP-TOKEN`)
- `DATABASE_URL` = Render Postgres connection string (must be async-compatible)
- `DYNAMIC_PAGES_DIR` = `config/dynamic_pages`

Optional:
- `APP_VERSION` = e.g. `5.0.0-phase1`
- `ENV` = `production`
- `SNAPSHOT_MAX_ROWS` = `1000`

---

## 2) Health
No token required:

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
