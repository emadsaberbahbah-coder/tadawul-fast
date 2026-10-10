# Offline calibration evidence

Export: `_Market Share Deepseek-V3 (1).xlsx`; SHA-256 `9c8b34812eee7d5554a3586f1f86a232e79159f24e228a4eac070e2436441ef4`.
Observed Global_Markets dominant update date: 2026-10-06; latest Signal_History date: 2026-10-06. These are source data dates, not a live read-back.
Performance_Log header row 5 is intentional.

After deduplication: 11,191 matured records; 5,130 wins / 5,857 losses, win rate 46.6915%. Brier 0.2840 over 10,987 eligible records (baseline 0.25).

Reliability is treated as an advisory implied probability. Blank/zero risk placeholders supply no risk evidence; their realized outcomes remain eligible. Signal_History supplies coverage only; ambiguous or later versions never replace entry predictions.

| Horizon | Origin page | Matured | Wins | Losses | Win rate % | Brier n | Brier | Mean claim % | Claim minus wins pp | Sufficient |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1M | Decision_Coverage | 863 | 423 | 437 | 49.1860 | 860 | 0.2889 | 54.2641 | 5.0780 | True |
| 1M | Top_10_Investments | 3204 | 1771 | 1385 | 56.1153 | 3156 | 0.2685 | 55.2155 | -0.8998 | True |
| 1W | Decision_Coverage | 1374 | 438 | 924 | 32.1586 | 1362 | 0.3165 | 58.1137 | 25.9551 | True |
| 1W | Top_10_Investments | 2010 | 986 | 939 | 51.2208 | 1925 | 0.2669 | 53.7224 | 2.5017 | True |
| 2W | Decision_Coverage | 1134 | 369 | 761 | 32.6549 | 1130 | 0.3194 | 57.7351 | 25.0803 | True |
| 2W | Top_10_Investments | 1859 | 853 | 955 | 47.1792 | 1808 | 0.2710 | 53.4655 | 6.2863 | True |
| 3M | Decision_Coverage | 26 | 10 | 16 | 38.4615 | 26 | 0.2900 | 51.5038 | 13.0423 | True |
| 3M | Top_10_Investments | 721 | 280 | 440 | 38.8889 | 720 | 0.3075 | 57.0089 | 18.1200 | True |

Exclusions and coverage:

```json
{
  "exclusions": {
    "duplicate_matured_excluded": 27,
    "matured_rows_raw": 11218,
    "non_matured_excluded": 8419,
    "performance_rows": 19637
  },
  "risk": {
    "all_zero_placeholder_triplets_excluded": 11191
  },
  "signals": {
    "ambiguous_entry_date_matches": 8161,
    "available": true,
    "by_page": {
      "Decision_Coverage": 14110,
      "Top_10_Investments": 14266
    },
    "entry_date_matches": 11191,
    "multi_version_symbol_days": 5156,
    "rows": 28376,
    "symbol_days": 7552
  }
}
```

Owner decision remains open: whether Brier above 0.25 should gate promotion. This report changes no production gate.
