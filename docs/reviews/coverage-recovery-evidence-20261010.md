# Coverage recovery requirements — 10 October 2026

This note uses the public GitHub audit results supplied for the next update.
It contains no private backup links, identifiers, checksums or workbook contents.

## Public audit evidence

The [Full Refresh Coverage audit](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38013029007)
rejected three pages at revision `309dda9`:

| Page | Rows | Existing minimum | Successful freshness | Finding |
| --- | ---: | ---: | ---: | --- |
| Market_Leaders | 255 | 1,025 | 99.2157% | 770-row arithmetic floor deficit |
| Global_Markets | 6,609 | 6,512 | 95.0976% | Name coverage 96.05%, below 99% |
| Mutual_Funds | 2,474 | 4,496 | 96.6855% | 2,022-row arithmetic floor deficit; name coverage 98.02%, below 99% |

Commodities_FX passed with 453 rows, and My_Portfolio passed with five rows.
Insights_Analysis and Data_Dictionary passed their structural checks.
These results are point-in-time audit evidence, not a post-update acceptance.

The [Decision Surface Freshness audit](https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/runs/38013029020)
reported `Executable: false`. Top10 ran at 03:16:58 Riyadh and preceded the
latest Global_Markets and Mutual_Funds source publications. It used 255 leaders
and 2,469 funds, both below their existing floors, while claiming a full universe.
Recent rows and successful job completion therefore did not prove a complete,
current decision surface.

## Recovery requirements

1. Locate and review an authoritative current membership roster. No approved
   restore manifest is supplied by this patch. An arithmetic deficit does not
   identify the missing instruments; historical membership requires review.
2. Back up the current workbook and review an exact symbol, exchange and currency
   diff before any restoration. Do not infer aliases, manufacture symbols or
   lower floors to obtain a green audit. The repository's build_universes tool
   explicitly cannot reconstruct these Saudi-native pages.
3. Verify issuer identity for missing names. Historical labels alone cannot
   override identity quarantine, wrong-instrument warnings or failed acquisition.
   Retain the 99% name coverage requirement and successful acquisition checks.
4. Complete all four source pages in one verified sync cohort. Refresh the
   installed native decision script after source completion, read back its
   versioned final state and rerun both live audits. The Python workflow does
   not execute the native cockpit refresh.

The update adds source-cohort checks and a final publication guard. It does not
restore membership or certify a repaired live workbook. Private backup evidence
remains outside this public repository deliverable.
