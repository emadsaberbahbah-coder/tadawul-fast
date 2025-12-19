# Deploy Checklist (Render + GitHub) — Phase-1 (DB-free)

## 1) GitHub Repo
✅ Files exist:
- `main.py`, `config.py`, `requirements.txt`, `render.yaml`, `runtime.txt`
- folders: `core/`, `routes/`, `dynamic/`, `domain/`, `config/dynamic_pages/`
- 9 YAML files in `config/dynamic_pages/`

✅ Build imports:
- `main.py` imports: health, config_routes, pages_routes, validate_routes, ksa.router, global_mkt.router
- No database required

---

## 2) Render Service Settings
✅ Build command:
- `pip install -r requirements.txt`

✅ Start command:
- `uvicorn main:app --host 0.0.0.0 --port $PORT`

✅ Environment Variables (minimum):
- `APP_TOKEN` = your secret token
- (Optional) `DYNAMIC_PAGES_DIR` = `config/dynamic_pages` (default already)

---

## 3) Live API Quick Tests
### Health (no token)
GET `/health`

### List pages (token)
GET `/api/v1/pages`
Header: `X-APP-TOKEN: <APP_TOKEN>`

### Load config (token)
GET `/api/v1/config/page_01_market_summary_ksa`
Header: `X-APP-TOKEN: <APP_TOKEN>`

### Validate page + sample (token)
POST `/api/v1/validate/page_01_market_summary_ksa`
Header: `X-APP-TOKEN: <APP_TOKEN>`
Body:
```json
{"sample_row":{"symbol":"1120.SR","currency":"SAR"}}
