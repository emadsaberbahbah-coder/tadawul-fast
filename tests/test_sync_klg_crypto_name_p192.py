"""WP-05 real-module last-good crypto name and final restoration regressions."""
from copy import deepcopy

import pytest

from core.symbols.crypto_identity import CRYPTO_NAME_TOKENS, crypto_name_matches
from scripts import run_dashboard_sync as sync
from scripts.critical_symbol_identity import CRYPTO_NAME_TOKENS as runner_tokens

HEADERS = ['Symbol', 'Name', 'Current Price', 'EPS (TTM)', 'P/E (TTM)', 'Data Provider', 'Warnings']


class Grid:
    def __init__(self, rows):
        self.rows = [HEADERS] + rows
        self.reads = 0

    def read_values(self, *_args):
        self.reads += 1
        return deepcopy(self.rows)


@pytest.mark.parametrize('raw,expected', [(None,'off'), ('1','off'), ('OBSERVE ','observe'), ('enforce','enforce'), ('junk','off')])
def test_explicit_modes(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv('TFB_SYNC_KLG_CRYPTO_NAME_GATE', raising=False)
    else:
        monkeypatch.setenv('TFB_SYNC_KLG_CRYPTO_NAME_GATE', raw)
    assert sync._klg_crypto_name_mode() == expected


@pytest.mark.parametrize('mode,swaps,name', [('off',['SUI-USD'],'Salmonation USD'), ('observe',['SUI-USD'],'Salmonation USD'), ('enforce',[],'')])
def test_mismatched_last_good_refused_only_enforced(monkeypatch, mode, swaps, name):
    monkeypatch.setenv('TFB_SYNC_KLG_CRYPTO_NAME_GATE', mode)
    prior = ['SUI-USD', 'Salmonation USD', 0.0001, '', '', 'yahoo_chart', '']
    incoming = ['SUI-USD', '', '', '', '', 'fallback_error', '']
    out, kept = sync._keep_last_good_rows(Grid([prior]), 'offline', 'Commodities_FX', HEADERS, [incoming])
    assert kept == swaps and out[0][1] == name
    if mode == 'enforce':
        assert 'SUI-USD' in sync._LAST_KLG_ID_SUSPECTS


def test_good_crypto_equity_unknown_and_fast_path(monkeypatch):
    monkeypatch.setenv('TFB_SYNC_KLG_CRYPTO_NAME_GATE', 'enforce')
    names = [('GRT-USD','The Graph USD'), ('AAPL.US','Apple'), ('UNLISTED-USD','Unknown Token')]
    prior = [[s,n,1.0,'','','yahoo_chart',''] for s,n in names]
    incoming = [[s,'','','','','fallback_error',''] for s,n in names]
    out, kept = sync._keep_last_good_rows(Grid(prior), 'offline', 'Commodities_FX', HEADERS, incoming)
    assert kept == [s for s,n in names]
    assert [r[1] for r in out] == [n for s,n in names]
    boundary = Grid(prior)
    healthy = deepcopy(prior)
    out, kept = sync._keep_last_good_rows(boundary, 'offline', 'Commodities_FX', HEADERS, healthy)
    assert boundary.reads == 0 and kept == [] and out == healthy


@pytest.mark.parametrize('mode', ['off','observe','enforce'])
def test_final_screen_survives_persistence_restoration(monkeypatch, mode):
    monkeypatch.setenv('TFB_SYNC_KLG_CRYPTO_NAME_GATE', mode)
    rows = [['UNI-USD','UNICORN Token USD',1.0,'','','yahoo_chart',''],
            ['UNI.US','Unrelated Equity',1.0,'','','eodhd',''],
            ['GRT-USD','', '', '', '', '', 'operator_quarantine_stub:v6.37.0']]
    original = deepcopy(rows)
    out, suspects = sync._crypto_name_firewall(HEADERS, rows)
    assert suspects == ([] if mode == 'off' else ['UNI-USD'])
    assert out[1:] == original[1:]
    if mode == 'enforce':
        assert out[0][:6] == ['UNI-USD','','','','','']
        assert out[0][6].startswith('identity_quarantined:crypto_name:')
        again, suspects = sync._crypto_name_firewall(HEADERS, deepcopy(out))
        assert again == out and suspects == []
    else:
        assert out == original


def test_registry_shared_without_semantic_drift():
    from scripts.tfb_export_audit import CRYPTO_NAMES
    assert runner_tokens == CRYPTO_NAMES == CRYPTO_NAME_TOKENS
    assert crypto_name_matches('APT-USD','Apricot Finance USD') is False
    assert crypto_name_matches('APT-USD','Aptos USD') is True
    assert crypto_name_matches('APT.US','Apricot Finance') is None
    # Existing substring vocabulary is not an identity oracle.
    assert crypto_name_matches('COMP-USD','Compound Coin USD') is True


def test_final_seam_is_after_restore_and_before_publication():
    import inspect
    source = inspect.getsource(sync)
    seam = source.index('# WP-05 final name screen:')
    assert source.index('# --- Persistence outcome verification') < seam
    assert seam < source.index('# v6.20.0 (Fix 1b): harvest', seam)
