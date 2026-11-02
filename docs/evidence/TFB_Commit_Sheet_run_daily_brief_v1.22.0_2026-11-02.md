# TFB Commit Sheet - run_daily_brief v1.22.0 [STATUS-READ-VETO]

| Item | Value |
| --- | --- |
| Date | 2026-11-02 (daily improvement routine) |
| Branch | `claude/daily-2026-11-02` |
| Base | origin/main `3bbf3c80` (run_daily_brief v1.20.0) |
| Lane | GitHub Actions only (`daily_brief.yml`); no Render deploy, no Apps Script paste |
| Gate | repository Variable `TFB_BRIEF_STATUS_READ_VETO` = on / 1 / true / yes; default off = v1.20.0 byte for byte |
| Priority | (a) a published recommendation is wrong: the emailed brief can offer executable tickets while the final decision feed withholds them |
| Version | 1.22.0; 1.21.0 is claimed by open #882, built on the same v1.20.0. Whichever merges second renumbers. |

## S1 - Defect and evidence

The Daily Investment Brief is emailed four times a day (`daily_brief.yml --send`). Its tickets are what the owner executes. The final TFB Decision Feed receipt on `_Status` guards them:

- `build_model` reads `_extract_final_feed(pages_data[_Status])`;
- `_apply_final_feed_veto` withholds every Portfolio_Decision SELL / TRIM / ADD when the feed is NOT_ACTIONABLE, WITHHELD or an expired release;
- `_top10_execution_veto` withholds the Top-10 picks on the same feed, else falls back to the Top_10 page's own status line.

`read_pages_live` (v1.20.0, lines 803-814) catches any page-read exception, writes `[brief] WARN could not read <page>: <exc>` and sets the page to `[]` ("one bad page must not kill the brief"). `integrations.google_sheets_service.read_range` re-raises after its retries, so a real Sheets outage reaches that branch. `[]` has no `TFB Decision Feed` key, so `_extract_final_feed` returns `''`, the veto is a no-op, and the brief renders the tickets.

Offline replay with the real module and a stubbed `read_range`, feed `NOT_ACTIONABLE(final_publication)`:

| _Status read | execution_veto | SELL / TRIM / ADD | Top-10 picks | subject |
| --- | --- | --- | --- | --- |
| ok | set | all withheld | withheld | no ticket |
| raises (HTTP 503) | none | EXIT.US / TRIM.US / ADD.US with shares, stops, targets | SELECTED.US | `... add ADD.US` |

`send_digest` is fail-closed on the same read: `_read_status_rows` / `_manifest_gate` report `_Status read failed`, and the digest withholds and still sends a research-only copy.

Live: latent. The four briefs of 1 Nov (runs 41315961253, 41328547569, 41341685978, 41358206360) logged no failed read.

Overlap checked:

- Codex PR #785 (and the identical copies in #791, #792, #795) carries an ungated v1.18.2 abort (`DecisionReceiptReadError`, no brief sent). It is based on 23 Oct and conflicted.
- #882 (run_daily_brief 1.21.0, unsized ADD) does not touch `read_pages_live` or `_Status`.

## S2 - Change

| Site | Change |
| --- | --- |
| `scripts/run_daily_brief.py` | v1.22.0. Dated WHY block above `__version__`, below the v1.20.0 block; prior blocks verbatim. |
| `read_pages_live` | In the existing `except` branch, only when `p == _Status` and the gate is on: the WARN line is kept, one more stderr line says the feed is treated as withheld, and the page becomes one receipt row `['TFB Decision Feed', 'WITHHELD(status_unreadable) \| _Status read failed: <ExceptionType>']`. The type name only, never the exception text (which can carry API detail). |
| `_status_read_veto_enabled` | Added: `TFB_BRIEF_STATUS_READ_VETO` in on / 1 / true / yes, any case. |
| `.github/workflows/daily_brief.yml` | `TFB_BRIEF_STATUS_READ_VETO: ${{ vars.TFB_BRIEF_STATUS_READ_VETO \|\| 'off' }}` in the "Render & send brief" step env, after `TFB_MAIL_TO`. Not armed. |
| `scripts/verify_deployment.py` | pin run_daily_brief 1.22.0 (pin only). |
| `.github/workflows/ci.yml` | 1.1.66 (1.1.62 / 1.1.63 claimed by open #778, 1.1.64 by #882, 1.1.65 by #889). A separate step in the blocking heavy job runs the harness, after fetching `3bbf3c80` for its golden leg. |
| `tests/test_daily_brief_send_window.py` | the exact `1.20.0` pin becomes a `>= (1, 20, 0)` floor. |

With the gate on, the existing machinery does the rest:

- `_feed_execution_veto` matches `WITHHELD(`;
- `_apply_final_feed_veto` moves every SELL / TRIM / ADD to `block` with 0 SAR, no shares, stop or targets, and 0 freed cash;
- `_top10_execution_veto` withholds the picks.

The brief still sends, with its "Final decision feed withheld execution" banner. That is `send_digest`'s withhold-and-send, not #785's abort.

Unchanged in every mode:

- every other page's read;
- a readable `_Status`, whatever its feed;
- a readable `_Status` with no `TFB Decision Feed` key (legacy: no veto).

Zero functions removed.

## S3 - Tests

`tests/test_daily_brief_status_read_veto.py`: **PASS 35/35** as a script, 35 passed under pytest. The brief suites together (blocked inputs, post-earnings tag, send window, this file): 99 passed.

| Case | Checks |
| --- | --- |
| S1 | off under 8 spellings (unset, off, 0, blank, spaces, onn, enforce, observe) equals the untouched v1.20.0 (`git show 3bbf3c80`) with `_Status` failing, readable, and EXECUTABLE. Compared: read output, stderr, decision, top10, text, HTML, subject. |
| S1b / S1c | the six on-spellings; junk stays off. |
| S2 | on, `_Status` raises: the one receipt row; every SELL / TRIM / ADD withheld (0 SAR, no shares / stop / targets, 0 freed cash); the picks withheld; text, HTML and PDF carry the banner and `status_unreadable`, never `HTTP 503` or SELECTED.US; the subject offers nothing; exactly two stderr lines. |
| S3 | on, `_Status` readable (EXECUTABLE, NOT_ACTIONABLE, no key, empty): identical to off. |
| S4 | on, Top_10, Portfolio_Decision or Calendar_Events fails: identical to off. |
| S5 | on, `HttpError` (with a key-like string), `TimeoutError`, `ValueError`: the receipt names the class only; the text never reaches any surface. |
| G1 | on the untouched v1.20.0 the same failed read gives no veto, EXIT / TRIM / ADD tickets, the SELECTED.US pick, `ADD.US` in the subject and BEST NEW BUYS in the text; S2's checks fail there. |
| V1 / V1b | version floor, verifier pin, the step-env mapping (one line, step env only), the ci.yml step, WHY order, ASCII file; against the base: ASCII added lines, the only removed line is the old `__version__`, exactly one function added, none removed. |

Golden negative of the whole file (`TFB_TEST_BRIEF_DELIVERED=<v1.20.0 copy>`): **26 failed, 8 passed, 1 skipped**. The 8 passes are by construction: S3 (4) and S4 (3) compare on with off, which are equal where the gate does not exist, and G1 targets the base. V1b applies to 1.22.0 only.

Mutants, each killed:

- the gate check removed: 8 failed;
- every failed page treated as `_Status`: 3;
- the exception text in the receipt: 4;
- the extra stderr line dropped: 1;
- a receipt word the veto does not know: 4;
- `yes` dropped from the gate words: 1.

On a shallow checkout without the base: 25 passed, 10 skipped visibly (S1 x8, G1, V1b).

Other checks:

- flag registry, workflow flag mappings, optional rollout flags, env contract, tracked flags, verify_deployment and the brief / digest suites: 142 passed, 1 skipped;
- compileall clean; ruff (CI rules) clean; workflow audit errors=0; `render_env_contract --check` OK.

## S4 - Operator steps

1. Merge. GitHub Actions only: no deploy, no paste.
2. Set the repository Variable `TFB_BRIEF_STATUS_READ_VETO=on`.
3. Read back: nothing changes while `_Status` reads normally. On a failed read, the run log shows two lines:
   - `[brief] WARN could not read _Status: ...`
   - `[brief] _Status unreadable - final decision feed treated as WITHHELD(status_unreadable)`

   The emailed brief then shows "Final decision feed withheld execution: WITHHELD(status_unreadable) | _Status read failed: <type>" and no ticket.

Rollback: unset the Variable (or set it off).

## S5 - Known limits

- Only a read that RAISES is covered. A readable `_Status` with no `TFB Decision Feed` key, or an empty grid, keeps the legacy no-veto. An observe line for that case is a possible follow-up.
- The offline xlsx path (`--xlsx`, `read_pages_xlsx`) is unchanged.
- With the gate on, one failed `_Status` read withholds a brief that might have been executable. That is the same trade `send_digest` makes. The next slot (3 h later) reads again.
- #882 and this PR both change the `__version__` line, the WHY position, the verifier pin, the floor in `test_daily_brief_send_window.py` and the ci.yml header. Whichever merges second resolves those few lines; the code seams do not overlap.

## S6 - Also found by today's survey (not built here)

- **URGENT: 123 verified Name bindings expire 2026-11-04 21:32Z.**
  - 43 of them are applied on Mutual_Funds every sync. Without them, Mutual_Funds Name coverage falls from ~99.6 % to ~97.9 %, under the 99 % Name floor, and the final publication blocks the decision feed. This repeats the 19 Oct incident (`renew_verified_name_labels.py` lines 3-6).
  - Owner action: dispatch `name_label_renewal.yml` with mode=renew and publish_branch=true, then review and merge the renewed registry before 4 Nov 21:32Z.
  - The Name Label Renewal checks 41445190822 and 41301306638 already report `status: EXPIRED`.
- **Weekly Decision Brief vs Shadow_Board.** The weekly memo evaluates the board without the shadow lane's env (retired screen, sheet id, engine ROI, read retries) and reads Shadow_Board columns by position. On 1 Nov it said NO_ACTION while Shadow_Board said SWITCH_CANDIDATES. Medium build; the memo is shadow mode and moves no capital.
- **Top_10_Investments_v2 Executable tile** (27_Decision_Pages_v2 1.2.31 Known limits): it counts Ticket SAR > 0 whatever the feed state, so it shows a green "EXECUTABLE 6" under an EXPIRED or NOT ACTIONABLE chip. A display contradiction, not a wrong number.
- **v_outcome_engine (views 1.5.0):** touched_target ignores the call's direction, and hourly target closes are scored as session closes.
- **Daily brief, failed Calendar_Events read:** it drops the earnings-proximity warning and the Calendar_Events fallback of the Best-new-buys earnings guard.
- **Outcome tracker:** no exchange calendar for .MX and other Yahoo suffixes, so those forecasts never mature.
