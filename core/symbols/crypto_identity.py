"""Crypto display-name vocabulary v1.0.0 (offline, pure).

WHY (2026-10-10 work brief WP-05): the audit knows collision-prone coin
names but last-good certification and fundamentals enrichment did not.
Extract the existing curated map without changing its substring semantics.
A match is a plausibility screen, never proof of a provider identifier.
"""
from __future__ import annotations

import re
from typing import Any

__version__ = "1.0.0"
CRYPTO_NAME_TOKENS = {
    "BTC": ["bitcoin"], "ETH": ["ethereum"], "SOL": ["solana"], "XRP": ["xrp", "ripple"],
    "BNB": ["bnb", "binance"], "ADA": ["cardano"], "DOGE": ["dogecoin"], "TRX": ["tron"],
    "AVAX": ["avalanche"], "DOT": ["polkadot"], "LINK": ["chainlink"], "MATIC": ["polygon"],
    "POL": ["polygon"], "LTC": ["litecoin"], "SHIB": ["shiba"], "UNI": ["uniswap"],
    "APT": ["aptos"], "SUI": ["sui"], "COMP": ["compound"], "ATOM": ["cosmos"],
    "NEAR": ["near"], "ARB": ["arbitrum"], "OP": ["optimism"], "AAVE": ["aave"],
    "MKR": ["maker"], "LDO": ["lido"], "TIA": ["celestia"], "EGLD": ["multiversx", "elrond"],
    "FIL": ["filecoin"], "ICP": ["internet computer"], "XLM": ["stellar"],
    "BCH": ["bitcoin cash"], "ETC": ["ethereum classic"], "HBAR": ["hedera"],
    "VET": ["vechain"], "ALGO": ["algorand"], "IMX": ["immutable"], "INJ": ["injective"],
    "RNDR": ["render"], "RENDER": ["render"], "GRT": ["graph"], "SAND": ["sandbox"],
    "MANA": ["decentraland"], "AXS": ["axie"], "FTM": ["fantom"], "XMR": ["monero"],
    "TON": ["toncoin", "ton"], "PEPE": ["pepe"], "SEI": ["sei"], "STX": ["stacks"],
    "KAS": ["kaspa"], "TAO": ["bittensor"], "USDT": ["tether"], "USDC": ["usd coin", "usdc"],
    "DAI": ["dai"], "CRO": ["cronos", "crypto.com"], "QNT": ["quant"], "THETA": ["theta"],
    "FLOW": ["flow"], "CHZ": ["chiliz"], "ENS": ["ethereum name"], "GALA": ["gala"],
    "APE": ["apecoin"], "CRV": ["curve"], "SNX": ["synthetix"], "RUNE": ["thorchain"],
    "KAVA": ["kava"], "ZEC": ["zcash"], "DASH": ["dash"], "NEO": ["neo"], "EOS": ["eos"],
    "XTZ": ["tezos"], "MINA": ["mina"], "ROSE": ["oasis"], "ONE": ["harmony"],
    "ZIL": ["zilliqa"], "ENJ": ["enjin"], "BAT": ["basic attention"], "1INCH": ["1inch"],
    "SUSHI": ["sushi"], "YFI": ["yearn"], "DYDX": ["dydx"], "GMX": ["gmx"],
    "PENDLE": ["pendle"], "JUP": ["jupiter"], "PYTH": ["pyth"], "ONDO": ["ondo"],
    "ENA": ["ethena"], "BONK": ["bonk"], "FLOKI": ["floki"], "AR": ["arweave"],
    "FET": ["fetch", "artificial superintelligence"], "WLD": ["worldcoin"],
    "BLUR": ["blur"], "CFX": ["conflux"], "MNT": ["mantle"], "STRK": ["starknet"],
    "AKT": ["akash"], "JASMY": ["jasmy"], "IOTA": ["iota"], "HNT": ["helium"],
    "RAY": ["raydium"], "CAKE": ["pancake"], "LUNC": ["terra"], "LUNA": ["terra"],
    "FXS": ["frax"], "BTT": ["bittorrent"], "NEXO": ["nexo"], "GNO": ["gnosis"],
    "RPL": ["rocket pool"], "CVX": ["convex"], "BAL": ["balancer"], "LRC": ["loopring"],
    "ANKR": ["ankr"], "CELO": ["celo"], "KSM": ["kusama"], "GLMR": ["moonbeam"],
    "ASTR": ["astar"], "WAVES": ["waves"], "ZRX": ["0x"], "QTUM": ["qtum"], "ICX": ["icon"],
    "ONT": ["ontology"], "STORJ": ["storj"], "SC": ["siacoin"], "DCR": ["decred"],
    "RVN": ["ravencoin"], "XEC": ["ecash"], "BSV": ["bitcoin sv"], "XEM": ["nem"],
    "LSK": ["lisk"], "DGB": ["digibyte"], "XNO": ["nano"], "HOT": ["holo"],
    "CKB": ["nervos"], "MASK": ["mask"], "LPT": ["livepeer"], "UMA": ["uma"],
    "KNC": ["kyber"], "PAXG": ["pax gold"], "XAUT": ["tether gold"], "TUSD": ["trueusd"],
    "FDUSD": ["first digital"], "PYUSD": ["paypal"], "WIF": ["dogwifhat", "wif"],
    "NOT": ["notcoin"], "ORDI": ["ordi"], "BEAM": ["beam"], "W": ["wormhole"],
    "ETHFI": ["ether.fi"], "AERO": ["aerodrome"], "ZK": ["zksync"], "TWT": ["trust wallet"],
}

_PAIR = re.compile(r"^([A-Z0-9]+)-([A-Z0-9]+)$")


def crypto_name_matches(symbol: Any, name: Any) -> bool | None:
    """False only for a curated pair with a definite token mismatch.

    Unknown roots and non-pairs are not judged. Blank names on curated
    pairs do not establish identity. Keep audit substring semantics exactly,
    including their documented ambiguity (e.g. Compound Coin matches COMP).
    """
    pair = _PAIR.fullmatch(str(symbol or "").strip().upper())
    tokens = CRYPTO_NAME_TOKENS.get(pair.group(1)) if pair else None
    if tokens is None:
        return None
    text = str(name or "").strip().lower()
    return any(token in text for token in tokens)
