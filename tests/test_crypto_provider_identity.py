"""WP-05 native crypto routing and real Yahoo fundamentals name firewall."""
import asyncio
from types import SimpleNamespace

import pytest

from core import data_engine_v2 as engine
from core.symbols import normalize


@pytest.fixture(autouse=True)
def clear_normalization_cache():
    normalize.to_eodhd_symbol.cache_clear()
    yield
    normalize.to_eodhd_symbol.cache_clear()


@pytest.mark.parametrize('symbol,legacy', [
    ('BTC-USD','BTC-USD'), ('SUI-USD','SUI-USD.US'), ('GRT-USD','GRT-USD.US'),
    ('APT-USD','APT-USD.US'), ('UNI-USD','UNI-USD'), ('COMP-USD','COMP-USD.US'),
    ('IMX-USD','IMX-USD.US'), ('STX-USD','STX-USD.US'),
])
def test_crypto_mapping_off_and_native_only_when_armed(monkeypatch, symbol, legacy):
    monkeypatch.delenv('TFB_EODHD_CRYPTO_MAP', raising=False)
    assert normalize.to_eodhd_symbol(symbol) == legacy
    monkeypatch.setenv('TFB_EODHD_CRYPTO_MAP','1')
    normalize.to_eodhd_symbol.cache_clear()
    assert normalize.to_eodhd_symbol(symbol) == symbol+'.CC'


@pytest.mark.parametrize('symbol', ['LINK.US','SUI','BRK-B.US','GC=F','EURUSD=X','UNKNOWN-USD','SUI-EUR'])
def test_other_instruments_unchanged(monkeypatch, symbol):
    monkeypatch.delenv('TFB_EODHD_CRYPTO_MAP', raising=False)
    before = normalize.to_eodhd_symbol(symbol)
    monkeypatch.setenv('TFB_EODHD_CRYPTO_MAP','1')
    normalize.to_eodhd_symbol.cache_clear()
    assert normalize.to_eodhd_symbol(symbol) == before


@pytest.mark.parametrize('symbol', ['BTC-USD','APT-USD','SUI-USD','GRT-USD','COMP-USD','IMX-USD','STX-USD'])
def test_engine_provider_normalization_chain_is_idempotent(monkeypatch, symbol):
    from core.providers.eodhd_provider import normalize_eodhd_symbol
    monkeypatch.setenv('TFB_EODHD_CRYPTO_MAP','1')
    routed = engine._provider_symbol_for('eodhd',symbol)
    assert routed == symbol+'.CC'
    assert normalize_eodhd_symbol(routed) == routed
    assert normalize.to_eodhd_symbol(routed) == routed
    assert engine._engine_patch_identity_mismatch(symbol, {'symbol':routed}) is None


@pytest.mark.parametrize('mode,accepted', [('off',True),('observe',True),('enforce',False)])
def test_real_fundamentals_channel_checks_name(monkeypatch, mode, accepted):
    patch = {'symbol':'APT-USD','shortName':'Apricot Finance USD','marketCap':42}
    provider = SimpleNamespace(fetch_fundamentals_patch=lambda _symbol: dict(patch))
    monkeypatch.setattr(engine, '_import_yahoo_provider_module', lambda _name: provider)
    monkeypatch.setattr(engine, '_yahoo_symbol_for', lambda symbol: symbol)
    monkeypatch.setenv('TFB_CRYPTO_NAME_GATE',mode)
    real = object.__new__(engine.DataEngineV5)
    result = asyncio.run(real._fetch_yahoo_fundamentals_patch('APT-USD'))
    assert result == (patch if accepted else {})


@pytest.mark.parametrize('symbol,name', [('APT-USD','Aptos USD'), ('AAPL.US','Apple'), ('UNKNOWN-USD','Unknown Token')])
def test_fundamentals_name_check_keeps_supported_and_unjudged(monkeypatch, symbol, name):
    monkeypatch.setenv('TFB_CRYPTO_NAME_GATE','enforce')
    assert engine._crypto_fundamentals_name_ok(symbol, {'name':name})
