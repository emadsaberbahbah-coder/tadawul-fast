# TFB Commit Sheet — news_macro_gate 1.11.0 [CENTRAL-BANKS] (three more official feeds: Bank of England, Bank of Japan, Swiss National Bank)

Date: 2026-11-26 (Thursday) · Lane: GitHub Actions (📰 News & Macro Gate) · Gate: observe-only — it controls no buy · No tab or header changes · No Render deploy

**Built on top of PR 1097 (origins, 1.10.0): merge that one first.**

## Why
Owner, 26 Nov 18:54 Riyadh (in Arabic): do the world market, the funds, commodities and currencies need the same care as the Saudi sources. 19:13: *"if it is ready, build it now"*.

The official group read the Fed, the ECB and US agencies, and no other central bank. Currencies and rates outside the dollar and the euro had no first-hand source.

## What
`scripts/news_macro_gate.py` 1.10.0 → **1.11.0**. Three feeds join the `official_rss` group (72-hour window, tier A):

| Id | Feed | Seen on 26 Nov |
|---|---|---|
| `boe` | `https://www.bankofengland.co.uk/rss/news` | RSS 2.0, 50 items, newest 25 Nov |
| `boj` | `https://www.boj.or.jp/en/rss/whatsnew.xml` | RSS 2.0, 46 items, newest 26 Nov |
| `snb` | `https://www.snb.ch/public/rss/en/pressrel` | RSS 2.0, 20 items, newest 24 Nov, date in `dc:date` |

Each bank publishes its feed for feed readers and allows personal, non-commercial use.

**A defect found on the way:** `domain_of()` read `www.boj.or.jp` as the domain `or.jp`, an unknown source (tier F). So a Bank of Japan page carried by Google News or GDELT was graded as an unknown blog. It now knows the Japanese second levels `or.jp` and `go.jp`.

## Left out on purpose
- **People's Bank of China:** its feed answers, but terms that allow automated reading were not found.
- **HKEX company announcements:** its terms forbid reading by a program without written permission.
- **SAMA, OPEC, IEA:** no official feed exists.
- **CNMV (Spain) company filings:** the feed is live and names each issuer; it needs a legal-name map for the Spanish names in scope, so it comes with the names work.

## Not yet proven
The feeds were opened from a cloud address, not from the GitHub runner. A feed that refuses the runner is named in the run's failure note, like every feed. The first dry run on main after merge settles it, and a feed that refuses is taken out.

The official group now asks nine feeds inside its 75-second budget, 12 seconds each at most. A slow day can leave the last feeds unasked; the note says how many.

## Tests
- `tests/test_news_macro_gate.py`: 322 pass. New G20:
  - the three registry rows, their window and tier;
  - the three date forms the banks write (RFC 822 ending `Z`, RFC 822 with `+0900`, ISO 8601 in `dc:date`);
  - an SNB-shaped feed parses with its dates; an old item and an undated item are left out and counted;
  - a bank that refuses is named and the others are still read;
  - `boj.or.jp`, `mof.go.jp` and `nhk.or.jp` keep their own domain.
- `tests/test_news_llm_reader.py`: 22 pass. Three hand mutants each fail a test.

## Owner
Merge PR 1097 first, then this one. Nothing else.

## Rollback
Revert the script to 1.10.0.

Not a licensed financial adviser; the owner decides and executes every trade.
