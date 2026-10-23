# Learning workflow reliability — 16 October 2026

Reviewed current main base `11ef1345` in the isolated workflow-review worktree.
Read GitHub job logs through the connected GitHub read API. No workflow was
dispatched, email sent, Sheet modified, production mode armed or policy selected.

## Observed outcomes

| Main run | Evidence from the actual job log | Interpretation |
| --- | --- | --- |
| Performance Clock `38793394337`, 15 October | `record_signal_snapshots: wrote 56 new daily snapshot(s)`; signal-history observe measured 56/56 repeated day keys, 30,758 rows / 8,151 keys, backlog 22,607. | The script ran and acknowledged appends. Observe is explicitly measurement-only; this is not deduplication or a fresh Sheet readback. |
| Shadow Scorer `38791915722`, 15 October | `NOT_DECIDABLE day 19/28 (+45 excluded-infra)`; `excluded_reason=fresh-floor`, challenger fresh 1/4; price errors zero; same-day board; no publication failure logged. | Successful execution with insufficient admissible model evidence. An S-1 policy failure/pending state is not a workflow runtime failure. |
| Shadow Board `38784013745`, 15 October | Six candidates, four eligible, two floor-locked, fundamentals 6/6, `risk=BREACH`, regime OK. | Domain risk/eligibility conclusions remain visible; they do not imply the job crashed. |
| Daily Brief `38814350872`, 15 October | HTML 27,386 bytes and PDF 7,524 bytes written, followed by email acknowledgement. | Render/send path executed; recipient details are omitted here. |
| Weekly Brief `38133134612`, 11 October | `mail=SENT:1`, ten candidates, `switch=NO_ACTION`. | Last weekly run acknowledged sending. Keeping holdings is a valid domain result. |
| Board Scorecard `38829879997`, 16 October | 330 episodes, 315 priced; Yahoo 168/168 symbols, benchmark OK; `mode=observe`, `wrote=0`, three artifacts. | Zero Sheet writes are intentional in observe mode. |
| Hypothesis backtest `38161414138`, 11 October | Announced 730 days for 16 symbols; every Yahoo request used `range=1mo&interval=1d`; all six hypotheses reported zero events. | Confirmed history-window wiring defect, separate from any decision about trigger definitions or promotion policy. |
| Investment Policy Shadow `38865163572`, 16 October | Actual engine candidate collection ran; strict final contract step passed after generating a nonempty selector report. | The generator's `continue-on-error` cannot alone make this workflow green: the subsequent always-run contract check requires nonempty `status=ok` evidence. No enforcement was applied. |

`gh run view --log` returned empty text for several jobs; direct job-log blob
redirects were inaccessible in this environment. The connected GitHub job-log
reader returned decoded logs, so the observations above use actual content,
not just green run conclusions. These are job-log acknowledgements, not broker,
live-portfolio or promotion certification.

## Repairs

`run_shadow_scorer.py` v1.9.4 continues attempting all four independently
guarded publications. Any failed publication now returns nonzero and records
an `ERROR` / `WRITE_FAILED` receipt when `_Run_Log` remains writable. A failure
of that receipt is also explicit and nonzero. Only fixed artifact names and
exception classes enter the new error diagnostics; private exception payloads
are excluded. Partial I/O is disclosed and no automatic rollback is attempted.
A successfully published S-1 `FAIL` still exits zero; criteria, outcome prices,
freshness floors, evidence windows and promotion decisions are unchanged.

`track_performance.py` v6.45.1 requires a positive registry-save acknowledgement
before claiming registration/results publication. Failed registration stops
before fetching/backtesting; failed result publication cannot print "results
written" or return success. A clear followed by a failed update remains a
possible partial operation and is reported honestly, without exposing its
exception payload. Existing in-memory backtest operation when no registry is
available is preserved; this change does not claim to certify remote storage.

The same tracker now adapts each history function's declared signature before
calling it. The Yahoo wrapper receives a supported `period` covering the
requested calendar days and daily bars; 730 days maps to `2y`. Days-aware
providers still receive the exact requested day count, and provider priority
remains Yahoo then EODHD. An internal `TypeError` no longer causes a second
call with the lookback argument silently removed. Returned history depths are
logged; a shallow/new-listing series remains insufficient and no bars, triggers
or returns are fabricated. This fixes input acquisition, not model performance.

## Verification

The two new files are `tests/test_learning_workflow_publication_failures.py`
and `tests/test_backtest_history_window.py`. Tests inject permission errors
into actual-main scorer publications, verify other writes still run, inspect
the error receipt and demonstrate a genuine clear-then-update partial failure.
They prove policy `FAIL` remains a successful publication, dry-run performs no
writes, and registry failures/refused acknowledgements never claim success.
The real Yahoo module wrapper/raw parser is exercised through a fake HTTP seam:
the request uses `2y`, the synthetic response supplies sufficient lookback,
and fallback is not called after Yahoo succeeds. Other fixtures preserve the
days-aware provider path and shallow-history insufficiency.

The initial focused run passed **96 tests** across the two new files plus
`test_shadow_scorer_regret_read_failure.py`; syntax compilation and whitespace
checks passed. Final validation after the shared dependency update belongs to
the root integration run. No production workflow has executed these repairs.
