#!/usr/bin/env python3
"""Critical symbol identity policy for the production market sync.

The sheet is the market-universe source. A poisoned row can therefore become a
permanent request and KEEP-LAST-GOOD can preserve it indefinitely. This module
contains the small, auditable set of symbol lifecycle and identity rules needed
for known collision cases. It performs no network calls and has no investment
or scoring logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, MutableSequence, Sequence

POLICY_VERSION = "1.2.0"
CRITICAL_IDENTITY_TAG = "identity_quarantined:critical_registry:v1.0.0"

# Provider-safe canonical identifiers. EODHD uses the .US exchange suffix and
# BRK-B.US for Berkshire Class B. Fiserv moved from FI to FISV in November 2025.
CANONICAL_SYMBOLS: Mapping[str, str] = {
    # v1.1.0 POLICY: BARE aliases (provider echoes / oracle shorthand) map to
    # the LIVE successor; SUFFIXED dead forms are retired via INACTIVE below
    # (a sheet row keeps its own symbol until the operator renames it —
    # auto-rewriting it in-flight would desync sheet vs write-matrix).
    #   BK  -> BNY.US   (NYSE ticker change effective 2026-05-21)
    #   FI  -> FISV.US  (NYSE->Nasdaq move effective 2025-11-11)
    #   BJK -> GENZ.US  (fund conversion effective 2026-04-09)
    "BK": "BNY.US",
    "BNY": "BNY.US",
    "FI": "FISV.US",
    "FISV": "FISV.US",
    "BJK": "GENZ.US",
    "GENZ": "GENZ.US",
    "BRK-B": "BRK-B.US",
    "BRK.B": "BRK-B.US",
    # ---- v1.2.0 (2026-08-09): 234 BARE US TICKERS CANONICALIZED ----
    # EVIDENCE: bare symbol 'AMX' took board seat 10 with a funded 18.2k SAR
    # ticket on the 2026-08-09 01:08 board; morning census of the live
    # Global_Markets export found 234 bare tickers, ALL exchange
    # 'NASDAQ/NYSE' country USA, and ZERO bare+'.US' collision pairs —
    # the remap is deterministic and collision-free by measurement.
    # EFFECT: providers fetch the EODHD-native '<SYM>.US' form (the bare
    # form is what pushed these rows onto fallback paths); per the v1.1.0
    # policy above the SHEET cell keeps its own symbol — no in-flight
    # rewrite, no sheet/write-matrix desync. Runner-side only: effective
    # from the first workflow run after this commit, no deploy needed.
    "A": "A.US",
    "AA": "AA.US",
    "ADSK": "ADSK.US",
    "AEM": "AEM.US",
    "AFL": "AFL.US",
    "AKAM": "AKAM.US",
    "ALGN": "ALGN.US",
    "AMCR": "AMCR.US",
    "AMX": "AMX.US",
    "AN": "AN.US",
    "AON": "AON.US",
    "APA": "APA.US",
    "APH": "APH.US",
    "ARGX": "ARGX.US",
    "ARM": "ARM.US",
    "ASML": "ASML.US",
    "ASX": "ASX.US",
    "AVB": "AVB.US",
    "AVGO": "AVGO.US",
    "AZN": "AZN.US",
    "BABA": "BABA.US",
    "BAC": "BAC.US",
    "BALL": "BALL.US",
    "BAM": "BAM.US",
    "BBD": "BBD.US",
    "BBVA": "BBVA.US",
    "BBY": "BBY.US",
    "BCS": "BCS.US",
    "BF-A": "BF-A.US",
    "BF-B": "BF-B.US",
    "BIIB": "BIIB.US",
    "BIRK": "BIRK.US",
    "BKR": "BKR.US",
    "BMO": "BMO.US",
    "BNS": "BNS.US",
    "BRO": "BRO.US",
    "BUD": "BUD.US",
    "BURL": "BURL.US",
    "CAT": "CAT.US",
    "CBRE": "CBRE.US",
    "CCI": "CCI.US",
    "CCL": "CCL.US",
    "CEG": "CEG.US",
    "CF": "CF.US",
    "CHD": "CHD.US",
    "CINF": "CINF.US",
    "CLX": "CLX.US",
    "CMG": "CMG.US",
    "CNC": "CNC.US",
    "CNI": "CNI.US",
    "CNQ": "CNQ.US",
    "COF": "COF.US",
    "COO": "COO.US",
    "CP": "CP.US",
    "CRH": "CRH.US",
    "CRM": "CRM.US",
    "CSX": "CSX.US",
    "CTSH": "CTSH.US",
    "CVNA": "CVNA.US",
    "CVX": "CVX.US",
    "CX": "CX.US",
    "D": "D.US",
    "DAL": "DAL.US",
    "DB": "DB.US",
    "DELL": "DELL.US",
    "DEO": "DEO.US",
    "DG": "DG.US",
    "DGX": "DGX.US",
    "DHI": "DHI.US",
    "DKNG": "DKNG.US",
    "DLR": "DLR.US",
    "DLTR": "DLTR.US",
    "DOCU": "DOCU.US",
    "DPZ": "DPZ.US",
    "DRI": "DRI.US",
    "DUK": "DUK.US",
    "DVN": "DVN.US",
    "EBAY": "EBAY.US",
    "ED": "ED.US",
    "EFX": "EFX.US",
    "EG": "EG.US",
    "ENTG": "ENTG.US",
    "EOG": "EOG.US",
    "EPAM": "EPAM.US",
    "EQR": "EQR.US",
    "EQT": "EQT.US",
    "EXPD": "EXPD.US",
    "EXPE": "EXPE.US",
    "EXR": "EXR.US",
    "FFIV": "FFIV.US",
    "FIS": "FIS.US",
    "FNV": "FNV.US",
    "FOXA": "FOXA.US",
    "FSLR": "FSLR.US",
    "FTNT": "FTNT.US",
    "GOOGL": "GOOGL.US",
    "GRMN": "GRMN.US",
    "GSK": "GSK.US",
    "HBAN": "HBAN.US",
    "HD": "HD.US",
    "HDB": "HDB.US",
    "HIG": "HIG.US",
    "HII": "HII.US",
    "HPE": "HPE.US",
    "ICE": "ICE.US",
    "IDXX": "IDXX.US",
    "IEX": "IEX.US",
    "IHG": "IHG.US",
    "INCY": "INCY.US",
    "ING": "ING.US",
    "INTU": "INTU.US",
    "IONQ": "IONQ.US",
    "IRM": "IRM.US",
    "JBHT": "JBHT.US",
    "JPM": "JPM.US",
    "KDP": "KDP.US",
    "KEY": "KEY.US",
    "KEYS": "KEYS.US",
    "KHC": "KHC.US",
    "KMI": "KMI.US",
    "KNSL": "KNSL.US",
    "KO": "KO.US",
    "LH": "LH.US",
    "LNG": "LNG.US",
    "LULU": "LULU.US",
    "LYB": "LYB.US",
    "LYV": "LYV.US",
    "MAA": "MAA.US",
    "MCD": "MCD.US",
    "MDB": "MDB.US",
    "META": "META.US",
    "MFG": "MFG.US",
    "MNTN": "MNTN.US",
    "MOS": "MOS.US",
    "MTB": "MTB.US",
    "MTCH": "MTCH.US",
    "MUFG": "MUFG.US",
    "NBIX": "NBIX.US",
    "NCLH": "NCLH.US",
    "NDAQ": "NDAQ.US",
    "NDSN": "NDSN.US",
    "NEE": "NEE.US",
    "NEM": "NEM.US",
    "NET": "NET.US",
    "NMR": "NMR.US",
    "NTR": "NTR.US",
    "NTRS": "NTRS.US",
    "NUE": "NUE.US",
    "NVDA": "NVDA.US",
    "NVR": "NVR.US",
    "O": "O.US",
    "OKTA": "OKTA.US",
    "OMC": "OMC.US",
    "OXY": "OXY.US",
    "PAYC": "PAYC.US",
    "PCG": "PCG.US",
    "PCTY": "PCTY.US",
    "PDD": "PDD.US",
    "PEG": "PEG.US",
    "PFE": "PFE.US",
    "PHM": "PHM.US",
    "PKG": "PKG.US",
    "PLD": "PLD.US",
    "PNC": "PNC.US",
    "POOL": "POOL.US",
    "PSX": "PSX.US",
    "RACE": "RACE.US",
    "RCL": "RCL.US",
    "RF": "RF.US",
    "RJF": "RJF.US",
    "RL": "RL.US",
    "RMD": "RMD.US",
    "RNR": "RNR.US",
    "ROST": "ROST.US",
    "RY": "RY.US",
    "S": "S.US",
    "SAP": "SAP.US",
    "SBAC": "SBAC.US",
    "SHW": "SHW.US",
    "SMCI": "SMCI.US",
    "SMFG": "SMFG.US",
    "SNA": "SNA.US",
    "SNAP": "SNAP.US",
    "SNOW": "SNOW.US",
    "SNY": "SNY.US",
    "SO": "SO.US",
    "SONY": "SONY.US",
    "SPG": "SPG.US",
    "STE": "STE.US",
    "STLA": "STLA.US",
    "STLD": "STLD.US",
    "SU": "SU.US",
    "SWK": "SWK.US",
    "SYF": "SYF.US",
    "T": "T.US",
    "TCOM": "TCOM.US",
    "TDY": "TDY.US",
    "TEAM": "TEAM.US",
    "TECK": "TECK.US",
    "TEL": "TEL.US",
    "TER": "TER.US",
    "TFC": "TFC.US",
    "TM": "TM.US",
    "TOL": "TOL.US",
    "TPR": "TPR.US",
    "TROW": "TROW.US",
    "TSCO": "TSCO.US",
    "TSN": "TSN.US",
    "TWLO": "TWLO.US",
    "TYL": "TYL.US",
    "UAL": "UAL.US",
    "UBS": "UBS.US",
    "UHS": "UHS.US",
    "ULTA": "ULTA.US",
    "UPS": "UPS.US",
    "USB": "USB.US",
    "VEEV": "VEEV.US",
    "VLTO": "VLTO.US",
    "VMC": "VMC.US",
    "VOD": "VOD.US",
    "VRSN": "VRSN.US",
    "VST": "VST.US",
    "WAT": "WAT.US",
    "WBD": "WBD.US",
    "WDAY": "WDAY.US",
    "WMB": "WMB.US",
    "WPM": "WPM.US",
    "WSM": "WSM.US",
    "WST": "WST.US",
    "WYNN": "WYNN.US",
    "XEL": "XEL.US",
    "XOM": "XOM.US",
    "XOP": "XOP.US",
    "ZBH": "ZBH.US",
}

# These identifiers must not remain in the active refresh universe.
# 3001.SR and 8270.SR are merger/delisting cases. 4328.SR has no verified active
# Saudi Exchange issuer mapping and is treated as unsupported until an official
# listing can be evidenced.
INACTIVE_SYMBOLS: Mapping[str, str] = {
    "BK.US": "ticker changed: BNY Mellon trades as BNY.US since 2026-05-21 — add BNY.US to the page to retain exposure",
    "FI.US": "ticker changed: Fiserv moved NYSE->Nasdaq as FISV.US since 2025-11-11 — add FISV.US to retain exposure",
    "BJK.US": "converted: VanEck Gaming ETF became Digital Native Economy ETF, GENZ.US, effective 2026-04-09 (mandate changed — operator decision whether to add GENZ.US)",
    "3001.SR": "delisted: Hail Cement acquired by Qassim Cement",
    "8270.SR": "inactive: Buruj merger into MEDGULF and trading suspension pending delisting",
    "4328.SR": "unsupported: no verified active Saudi Exchange issuer mapping",
}


@dataclass(frozen=True)
class IdentityRule:
    accepted_name_tokens: tuple[str, ...]
    currency_tokens: tuple[str, ...] = ("usd",)
    country_tokens: tuple[str, ...] = ("usa", "united states")
    exchange_tokens: tuple[str, ...] = ()


CRITICAL_IDENTITIES: Mapping[str, IdentityRule] = {
    "BNY.US": IdentityRule(
        accepted_name_tokens=("bny", "bank of new york mellon"),
        exchange_tokens=("nyse",),
    ),
    "BRK-B.US": IdentityRule(
        accepted_name_tokens=("berkshire hathaway",),
        exchange_tokens=("nyse",),
    ),
    "FISV.US": IdentityRule(
        accepted_name_tokens=("fiserv",),
        exchange_tokens=("nasdaq",),
    ),
}

CRITICAL_FETCH_SYMBOLS = frozenset(CRITICAL_IDENTITIES)


@dataclass(frozen=True)
class UniverseChange:
    source_symbol: str
    action: str
    target_symbol: str = ""
    reason: str = ""


@dataclass(frozen=True)
class IdentityFailure:
    symbol: str
    reason: str
    seen_name: str = ""


def normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def canonicalize_symbol(value: Any) -> str:
    symbol = normalize_symbol(value)
    return CANONICAL_SYMBOLS.get(symbol, symbol)


def sanitize_active_universe(symbols: Iterable[Any]) -> tuple[list[str], list[UniverseChange]]:
    """Remove inactive identifiers, canonicalize collision-prone US tickers,
    and de-duplicate stably.

    The returned list is the only list that should be sent to providers and to
    persistence verification. Removing a retired symbol here prevents the old
    poisoned row from being made immortal by the persistence layer.
    """
    clean: list[str] = []
    changes: list[UniverseChange] = []
    seen: set[str] = set()

    for raw in symbols:
        source = normalize_symbol(raw)
        if not source:
            continue
        if source in INACTIVE_SYMBOLS:
            changes.append(
                UniverseChange(
                    source_symbol=source,
                    action="removed",
                    reason=INACTIVE_SYMBOLS[source],
                )
            )
            continue

        target = canonicalize_symbol(source)
        if target != source:
            changes.append(
                UniverseChange(
                    source_symbol=source,
                    action="canonicalized",
                    target_symbol=target,
                    reason="provider-safe current identifier",
                )
            )

        if target in seen:
            changes.append(
                UniverseChange(
                    source_symbol=source,
                    action="deduplicated",
                    target_symbol=target,
                    reason="canonical symbol already present",
                )
            )
            continue
        seen.add(target)
        clean.append(target)

    return clean, changes


def build_isolated_batches(symbols: Sequence[Any], batch_size: int) -> list[list[str]]:
    """Put every critical identifier in its own provider request.

    Critical requests run first so a page time budget cannot starve the repair.
    Non-critical symbols retain their relative order and normal batch size.
    """
    size = max(1, int(batch_size))
    normalized = [normalize_symbol(s) for s in symbols if normalize_symbol(s)]
    critical = [[s] for s in normalized if s in CRITICAL_FETCH_SYMBOLS]
    normal = [s for s in normalized if s not in CRITICAL_FETCH_SYMBOLS]
    return critical + [normal[i : i + size] for i in range(0, len(normal), size)]


def _norm_cell(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().split())


def _find_column(headers: Sequence[Any], aliases: Sequence[str]) -> int:
    wanted = {"".join(ch for ch in alias.casefold() if ch.isalnum()) for alias in aliases}
    for index, header in enumerate(headers):
        norm = "".join(ch for ch in str(header or "").casefold() if ch.isalnum())
        if norm in wanted:
            return index
    return -1


def _optional_field_matches(value: Any, accepted: Sequence[str]) -> bool:
    if not accepted:
        return True
    text = _norm_cell(value)
    if not text:
        return True
    return any(token in text for token in accepted)


def quarantine_critical_rows(
    headers: Sequence[Any],
    rows: MutableSequence[list[Any]],
) -> tuple[MutableSequence[list[Any]], list[IdentityFailure]]:
    """Fail closed on a known critical Symbol->Issuer mismatch.

    A failing row is converted to a symbol-only stub with a visible warning. The
    caller must also mark the page result failed after the write; writing the
    stub purges an already-poisoned predecessor while the failed result prevents
    a false-green refresh verdict.
    """
    failures: list[IdentityFailure] = []
    if not headers or rows is None:
        return rows, failures

    sym_i = _find_column(headers, ("Symbol", "Ticker", "Code"))
    name_i = _find_column(headers, ("Name", "Company Name", "Instrument Name", "Short Name"))
    currency_i = _find_column(headers, ("Currency", "Currency Code"))
    country_i = _find_column(headers, ("Country", "Country Name"))
    exchange_i = _find_column(headers, ("Exchange", "Market", "Exchange Code"))
    warning_i = _find_column(headers, ("Warnings", "Warning"))
    if sym_i < 0:
        return rows, failures

    for row_index, row in enumerate(list(rows)):
        if not isinstance(row, list) or sym_i >= len(row):
            continue
        # Provider responses are not guaranteed to echo the current request
        # spelling.  Resolve aliases here (rather than relying on the batched
        # fetcher) so the same rule is selected on every call path.
        symbol = canonicalize_symbol(row[sym_i])
        rule = CRITICAL_IDENTITIES.get(symbol)
        if rule is None:
            continue
        row[sym_i] = symbol

        name = row[name_i] if 0 <= name_i < len(row) else ""
        name_text = _norm_cell(name)
        reason = ""
        if not name_text:
            reason = "blank instrument name"
        elif not any(token in name_text for token in rule.accepted_name_tokens):
            reason = "issuer name mismatch"
        elif currency_i >= 0 and not _optional_field_matches(
            row[currency_i] if currency_i < len(row) else "", rule.currency_tokens
        ):
            reason = "currency mismatch"
        elif country_i >= 0 and not _optional_field_matches(
            row[country_i] if country_i < len(row) else "", rule.country_tokens
        ):
            reason = "country mismatch"
        elif exchange_i >= 0 and not _optional_field_matches(
            row[exchange_i] if exchange_i < len(row) else "", rule.exchange_tokens
        ):
            reason = "exchange mismatch"

        if not reason:
            continue

        blanked = ["" for _ in row]
        blanked[sym_i] = row[sym_i]
        if 0 <= warning_i < len(blanked):
            blanked[warning_i] = CRITICAL_IDENTITY_TAG
        rows[row_index] = blanked
        failures.append(
            IdentityFailure(symbol=symbol, reason=reason, seen_name=str(name or "")[:100])
        )

    return rows, failures


def validate_fresh_critical_rows(
    headers: Sequence[Any],
    rows: MutableSequence[list[Any]],
    requested_symbols: Iterable[Any],
) -> tuple[MutableSequence[list[Any]], list[IdentityFailure]]:
    """Validate current-run proof for every requested critical identifier.

    This must run directly after response membership filtering, before any
    persistence or KEEP-LAST-GOOD operation can add a predecessor row.  A
    valid predecessor protects stored data, but is deliberately not evidence
    that the provider returned the right instrument in this run.
    """
    requested = {
        canonicalize_symbol(symbol)
        for symbol in requested_symbols
        if canonicalize_symbol(symbol) in CRITICAL_FETCH_SYMBOLS
    }
    rows, failures = quarantine_critical_rows(headers, rows)
    failed = {failure.symbol for failure in failures}

    sym_i = _find_column(headers, ("Symbol", "Ticker", "Code"))
    returned: set[str] = set()
    if sym_i >= 0:
        for row in rows:
            if not isinstance(row, list) or sym_i >= len(row):
                continue
            symbol = canonicalize_symbol(row[sym_i])
            if symbol in requested:
                row[sym_i] = symbol
                returned.add(symbol)

    for symbol in sorted(requested - returned - failed):
        failures.append(IdentityFailure(symbol=symbol, reason="missing fresh response row"))
    return rows, failures


def fail_result_on_identity(result: Any, failures: Sequence[IdentityFailure]) -> Any:
    """Ensure an unrecoverable critical quarantine can never report success."""
    if not failures:
        return result
    symbols = ", ".join(f.symbol for f in failures)
    result.status = "failed"
    result.rows_failed = max(int(getattr(result, "rows_failed", 0) or 0), len(failures))
    result.error = f"Critical symbol identity mismatch: {symbols}"
    return result
