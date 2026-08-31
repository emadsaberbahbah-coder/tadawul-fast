# Render Dashboard Evidence Checklist

This checklist captures configuration **names and state**, never secret values.

## Web service

- Service name and service ID
- Region and instance type/plan
- Repository and auto-deploy branch
- Deployed commit SHA
- Build command
- Start command
- Health check path
- Auto-deploy enabled/disabled
- Number of instances
- `PORT`, `WEB_CONCURRENCY`, `WORKERS_MAX`, timeout and keep-alive variable names
- Effective guard-state fingerprint from `verify_deployment.py --json --strict`
- `/readyz` response and release identity

## Worker service

- Confirm that a distinct Render worker/background service actually exists
- Service name and deployed SHA
- Start command (`python3 scripts/worker.py` or reviewed equivalent)
- Redis connection presence and queue name
- Dead-letter queue name
- Fail-fast behavior when Redis is unavailable
- Last successful job timestamp and current queue depth

## Data services

- Redis resource name/region/plan
- PostgreSQL resource name/region/plan when used
- Network relationship between web, worker, Redis, and database
- Backup/retention configuration

## Environment groups

Record only variable names and non-sensitive booleans/numbers. For secrets, record:

- variable name;
- present/missing;
- source (service-level or environment group);
- last rotation date if known;
- never the value.

## Release acceptance

A deployment is not certified until all of the following identify the same release:

- GitHub reviewed commit SHA;
- Render deployed SHA;
- `verify_deployment.py` module manifest;
- health/version payload;
- `_Status` release/cohort record;
- persisted Google Sheets validation artifact.
