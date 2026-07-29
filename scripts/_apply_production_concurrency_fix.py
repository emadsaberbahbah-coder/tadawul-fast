#!/usr/bin/env python3
# One-shot, fail-closed branch helper for concurrency isolation and inline recovery.
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAILY = ROOT / ".github" / "workflows" / "daily_sync.yml"
RECOVERY = ROOT / ".github" / "workflows" / "page_refresh_recovery.yml"
SELF = ROOT / "scripts" / "_apply_production_concurrency_fix.py"
HELPER = ROOT / ".github" / "workflows" / "_apply_production_concurrency_fix.yml"

CONCURRENCY_SECTION = r'''# ============================================================================
# CONCURRENCY — PRODUCTION WRITE LEASE, CI ISOLATED
# ============================================================================
# WHY (2026-07-29): push/PR runs execute tests only and never write the workbook.
# Putting those CI-only runs in the production write group caused GitHub's
# one-running/one-pending concurrency rule to cancel both Advanced Sync and Page
# Refresh Recovery runs before they started. Scheduled and manual production syncs
# must still share one lease to prevent chimeric Google Sheet writes.
#
# Production: schedule + workflow_dispatch share one non-cancelling write lease.
# CI only: push + pull_request use their own cancellable group and never block data
# refresh or recovery.
# ============================================================================
concurrency:
  group: ${{ (github.event_name == 'schedule' || github.event_name == 'workflow_dispatch') && format('tadawul-production-write-{0}', github.ref) || format('tadawul-ci-{0}-{1}-{2}', github.workflow, github.event_name, github.ref) }}
  cancel-in-progress: ${{ github.event_name == 'push' || github.event_name == 'pull_request' }}
'''

INLINE_RECOVERY_JOB = r'''  # =============================================================================
  # AUTOMATIC INLINE PAGE RECOVERY — retains the same production write lease
  # =============================================================================
  recover-missing-market-pages:
    name: 🧯 Recover missing market pages
    needs: sync-dashboard
    if: >-
      ${{
        always() &&
        (
          github.event_name == 'schedule' ||
          (
            github.event_name == 'workflow_dispatch' &&
            github.event.inputs.run_mode == 'full_sync'
          )
        )
      }}
    runs-on: ubuntu-latest
    environment: production
    timeout-minutes: 360
    env:
      PYTHON_VERSION: "3.11"
      PYTHONUNBUFFERED: "1"
      PYTHONPATH: ${{ github.workspace }}
      TZ: Asia/Riyadh
      PIP_DISABLE_PIP_VERSION_CHECK: "1"
      TARGET_URL: ${{ vars.BACKEND_BASE_URL || 'https://tadawul-fast-bridge.onrender.com' }}
      TARGET_SHEET_ID: ${{ vars.DEFAULT_SPREADSHEET_ID }}
      TFB_XPAGE_PRICE_CHECK: "1"
      TFB_SYNC_EMPTY_RETRY: "0"
      TFB_SYNC_NAME_DEDUP_MODE: "observe"
      TFB_PORTFOLIO_REBUILD: "1"
      TFB_SYNC_DECISION_GUARD: "1"
      TFB_SYNC_SYMBOL_BATCH_SIZE: "25"
      TFB_SYNC_TIME_BUDGET_SEC: "3600"
      TFB_SYNC_MAX_SYMBOLS_MARKET: "7000"
      TFB_SYNC_MARKET_GATEWAY: "analysis"
      BACKEND_TOKEN: ${{ secrets.BACKEND_TOKEN }}

    steps:
      - name: 📥 Secure Checkout
        uses: actions/checkout@v6
        with:
          fetch-depth: 1
          persist-credentials: false

      - name: 🐍 Setup Python 3.11
        uses: actions/setup-python@v6
        with:
          python-version: "3.11"
          cache: "pip"
          cache-dependency-path: |
            requirements.render.txt
            requirements.txt

      - name: 📦 Install Dependencies
        run: |
          set -euo pipefail
          python -m pip install --upgrade pip setuptools wheel
          if [[ -f requirements.render.txt ]]; then
            python -m pip install -r requirements.render.txt
          elif [[ -f requirements.txt ]]; then
            python -m pip install -r requirements.txt
          else
            echo "::error::No requirements file found."
            exit 1
          fi

      - name: 📥 Download current sync evidence
        uses: actions/download-artifact@v8
        with:
          pattern: tadawul-sync-logs-${{ github.run_id }}-*
          path: source-sync-artifacts
          merge-multiple: false

      - name: 🔐 Configure production credentials
        env:
          SECRET_CREDS: ${{ secrets.GOOGLE_SHEETS_CREDENTIALS }}
          SECRET_CREDS_B64: ${{ secrets.GOOGLE_SHEETS_CREDENTIALS_B64 }}
        run: |
          set -euo pipefail
          if [[ -z "${TARGET_SHEET_ID:-}" ]]; then
            echo "::error::vars.DEFAULT_SPREADSHEET_ID is empty."
            exit 1
          fi
          RAW_CREDS="${SECRET_CREDS:-}"
          if [[ -z "$RAW_CREDS" && -n "${SECRET_CREDS_B64:-}" ]]; then
            RAW_CREDS="$(printf '%s' "$SECRET_CREDS_B64" | base64 --decode 2>/dev/null || true)"
          fi
          if [[ -z "$RAW_CREDS" ]] || ! printf '%s' "$RAW_CREDS" | jq empty >/dev/null 2>&1; then
            echo "::error::No valid Google Sheets credentials found."
            exit 1
          fi
          CREDS_PATH="$RUNNER_TEMP/google_credentials.json"
          printf '%s' "$RAW_CREDS" > "$CREDS_PATH"
          chmod 600 "$CREDS_PATH"
          SA_EMAIL="$(jq -r '.client_email // empty' "$CREDS_PATH")"
          [[ -n "$SA_EMAIL" ]] && echo "::add-mask::$SA_EMAIL"
          echo "GOOGLE_APPLICATION_CREDENTIALS=$CREDS_PATH" >> "$GITHUB_ENV"

      - name: 💓 Check backend health
        run: |
          set -euo pipefail
          for endpoint in /readyz /health /livez; do
            if curl -fsS --max-time 15 "$TARGET_URL$endpoint" >/dev/null; then
              echo "Backend healthy via $endpoint"
              exit 0
            fi
          done
          echo "::error::Backend health checks failed."
          exit 1

      - name: 🧯 Recover only pages that did not refresh
        run: |
          set -euo pipefail
          python scripts/run_inline_page_recovery.py \
            --source-root source-sync-artifacts \
            --backend "$TARGET_URL" \
            --sheet-id "$TARGET_SHEET_ID" \
            --evidence-root inline-recovery-evidence \
            --plan-out inline-recovery-plan.json \
            --summary-out inline-recovery-summary.json

      - name: 📤 Upload recovery plan and summary
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: inline-recovery-${{ github.run_id }}
          path: |
            inline-recovery-plan.json
            inline-recovery-summary.json
          retention-days: 30
          if-no-files-found: warn

      - name: 📤 Upload Global Markets evidence
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: page-refresh-${{ github.run_id }}-global-markets
          path: inline-recovery-evidence/global-markets
          retention-days: 30
          if-no-files-found: ignore

      - name: 📤 Upload Commodities/FX evidence
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: page-refresh-${{ github.run_id }}-commodities-fx
          path: inline-recovery-evidence/commodities-fx
          retention-days: 30
          if-no-files-found: ignore

      - name: 📤 Upload Mutual Funds evidence
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: page-refresh-${{ github.run_id }}-mutual-funds
          path: inline-recovery-evidence/mutual-funds
          retention-days: 30
          if-no-files-found: ignore

      - name: 📤 Upload Market Leaders evidence
        if: always()
        uses: actions/upload-artifact@v7
        with:
          name: page-refresh-${{ github.run_id }}-market-leaders
          path: inline-recovery-evidence/market-leaders
          retention-days: 30
          if-no-files-found: ignore

      - name: 🧹 Secure Cleanup
        if: always()
        run: |
          CREDS_PATH="${GOOGLE_APPLICATION_CREDENTIALS:-$RUNNER_TEMP/google_credentials.json}"
          if [[ -f "$CREDS_PATH" ]]; then
            shred -u -z "$CREDS_PATH" 2>/dev/null || rm -f "$CREDS_PATH"
          fi
'''


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


def patch_daily() -> None:
    text = DAILY.read_text(encoding="utf-8")
    start = text.index(
        "# ============================================================================\n"
        "# CONCURRENCY — SINGLE WRITE LEASE ON THE PRODUCTION WORKBOOK"
    )
    end = text.index("\npermissions:", start)
    text = text[:start] + CONCURRENCY_SECTION.rstrip() + text[end:]

    text = replace_once(
        text,
        'python -m pytest -q "$SCORE_TEST" tests/test_schema_alignment.py tests/test_recent_fixes.py',
        'python -m pytest -q "$SCORE_TEST" tests/test_schema_alignment.py tests/test_recent_fixes.py tests/test_inline_page_recovery.py',
        "daily CI test list",
    )
    text = text.replace("actions/upload-artifact@v5", "actions/upload-artifact@v7")

    if "recover-missing-market-pages:" in text:
        raise SystemExit("daily workflow already contains inline recovery")
    text = text.rstrip() + "\n\n" + INLINE_RECOVERY_JOB.rstrip() + "\n"
    DAILY.write_text(text, encoding="utf-8")


def patch_recovery() -> None:
    text = RECOVERY.read_text(encoding="utf-8")
    text = replace_once(
        text,
        "  workflow_run:\n"
        "    workflows: ['🏦 Tadawul Dashboard Advanced Sync']\n"
        "    types: [completed]\n",
        "",
        "remove automatic workflow_run trigger",
    )

    old_concurrency = '''# Match the production workflow's resolved concurrency key. A recovery run starts
# after its source run, and this shared key also prevents a manual/scheduled sync
# from writing the production workbook while recovery is active.
concurrency:
  group: 🏦 Tadawul Dashboard Advanced Sync-${{ github.ref }}
  cancel-in-progress: false
'''
    new_concurrency = '''# Automatic recovery now runs inside Advanced Sync and keeps the same production
# lease. This workflow remains for manual historical-run recovery only. Its CI
# tests use a separate cancellable group and never compete with production writes.
concurrency:
  group: ${{ github.event_name == 'workflow_dispatch' && format('tadawul-production-write-{0}', github.ref) || format('page-recovery-ci-{0}-{1}', github.event_name, github.ref) }}
  cancel-in-progress: ${{ github.event_name == 'push' || github.event_name == 'pull_request' }}
'''
    text = replace_once(text, old_concurrency, new_concurrency, "recovery concurrency")

    plan_start = text.index("  plan:\n")
    if_start = text.index("    if: >-\n", plan_start)
    runs_on = text.index("    runs-on: ubuntu-latest\n", if_start)
    text = (
        text[:if_start]
        + "    if: ${{ github.event_name == 'workflow_dispatch' }}\n"
        + text[runs_on:]
    )

    for anchor, addition in (
        (
            "      - 'scripts/plan_sync_recovery.py'\n",
            "      - 'scripts/plan_sync_recovery.py'\n"
            "      - 'scripts/run_inline_page_recovery.py'\n",
        ),
        (
            "      - 'tests/test_sync_recovery_plan.py'\n",
            "      - 'tests/test_sync_recovery_plan.py'\n"
            "      - 'tests/test_inline_page_recovery.py'\n",
        ),
    ):
        expected = 2
        count = text.count(anchor)
        if count != expected:
            raise SystemExit(
                f"recovery paths for {anchor.strip()}: expected {expected}, found {count}"
            )
        text = text.replace(anchor, addition)

    text = replace_once(
        text,
        "            scripts/plan_sync_recovery.py \\\n"
        "            tests/test_sync_outcome_audit.py \\\n"
        "            tests/test_sync_recovery_plan.py",
        "            scripts/plan_sync_recovery.py \\\n"
        "            scripts/run_inline_page_recovery.py \\\n"
        "            tests/test_sync_outcome_audit.py \\\n"
        "            tests/test_sync_recovery_plan.py \\\n"
        "            tests/test_inline_page_recovery.py",
        "recovery compile list",
    )
    text = replace_once(
        text,
        "            tests/test_sync_outcome_audit.py \\\n"
        "            tests/test_sync_recovery_plan.py",
        "            tests/test_sync_outcome_audit.py \\\n"
        "            tests/test_sync_recovery_plan.py \\\n"
        "            tests/test_inline_page_recovery.py",
        "recovery unittest list",
    )
    RECOVERY.write_text(text, encoding="utf-8")


def main() -> None:
    patch_daily()
    patch_recovery()
    SELF.unlink(missing_ok=True)
    HELPER.unlink(missing_ok=True)
    print("Applied production concurrency isolation and inline recovery.")


if __name__ == "__main__":
    main()
