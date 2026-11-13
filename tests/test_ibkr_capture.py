"""scripts/ibkr_capture.py v1.0.0 - the read-only IBKR capture for the plan pages (owner, 13 Nov 2026: the action plan
should follow the cash available at IBKR). Zero network: a fake Drive session."""
import datetime as dt
import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import ibkr_capture as ic  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 13, 9, 0, tzinfo=UTC)             # 12:00 Riyadh
CREATED = dt.datetime(2026, 11, 13, 5, 41, 17, tzinfo=UTC)


def order(desc, typ, side, second, status="NEW", total="100", rem=None, limit=None):
    o = {"order_id": 1, "order_status": status, "order_type": typ, "side": side, "total_shares_qty": total,
         "cum_shares_qty": "0", "remaining_shares_qty": rem if rem is not None else total,
         "primary_description": desc, "secondary_description": second, "order_time": "2026-11-12T14:24:40Z"}
    if limit is not None:
        o["limit_price"] = limit
    return o


def capture(**over):
    c = {"schema": "tfb.ibkr-capture/v1", "captured_at": "2026-11-13T08:41:01.522873+03:00", "account_alias": "ibkr-primary",
         "positions": {"positions": [
             {"contract_description": "DDI", "position": 240.0, "market_price": 15.7},
             {"contract_description": "GLPI", "position": 79.0}, {"contract_description": "NMM", "position": 24.0},
             {"contract_description": "BRK B", "position": 3.0}, {"contract_description": "FLAT", "position": 0.0}]},
         "balances": {"balances": [
             {"currency": "BASE", "cash_balance": 4679.15, "settled_cash": 4679.15, "net_liquidation_value": 22071.82,
              "stock_market_value": 17361.74},
             {"currency": "USD", "cash_balance": 4679.15, "settled_cash": 4500.0, "net_liquidation_value": 22071.82,
              "stock_market_value": 17361.74}]},
         "orders": {"orders": [
             order("Sell 120 DDI", "STOP", "SELL", "STP 14.80, GTC", status="REPLACED", total="120"),
             order("Sell 79 GLPI", "STOP", "SELL", "STP 35.74, GTC", total="79"),
             order("Sell 79 GLPI", "STP LMT", "SELL", "STP LMT 35.10 34.90, GTC", total="79", rem="40"),
             order("Sell 24 NMM", "LIMIT", "SELL", "Limit 99.50, GTC", total="24", limit="99.5"),
             order("Sell 24 NMM", "STOP", "SELL", "STP 1,080.98, GTC", total="24"),
             order("Buy 20 INSW", "LIMIT", "BUY", "Limit 99.40, Day", total="20", limit="99.4"),
             order("Buy 5 AX", "MARKET", "BUY", "Market, Day", total="5"),
             order("Sell 10 OLD", "STOP", "SELL", "STP 5.00, GTC", status="CANCELLED", total="10"),
             order("Sell 10 GONE", "LIMIT", "SELL", "Limit 9, GTC", status="FILLED", total="10", limit="9"),
             order("something else", "STOP", "SELL", "STP 1, GTC")]}}
    c.update(over)
    return c


def test_c1_parse_reads_cash_positions_and_working_orders():
    p = ic.parse(capture(), CREATED, NOW, "TFB_IBKR_Capture_20261113-084101.json")
    assert p["ok"] and p["reason"] == "" and p["name"].endswith("084101.json")
    assert p["as_of"] == dt.datetime(2026, 11, 13, 5, 41, 1, 522873, tzinfo=UTC) and round(p["age_h"], 2) == 3.32
    assert (p["cash_usd"], p["settled_usd"], p["nav_usd"], p["stock_usd"]) == (4679.15, 4500.0, 22071.82, 17361.74)   # USD row
    assert p["positions"] == {"DDI.US": 240.0, "GLPI.US": 79.0, "NMM.US": 24.0, "BRK.B": 3.0}       # flat position dropped
    assert p["stops"] == {"DDI.US": [(14.8, 120.0)], "GLPI.US": [(35.74, 79.0), (35.1, 40.0)], "NMM.US": [(1080.98, 24.0)]}
    assert p["limits"] == {"NMM.US": [(99.5, 24.0)]}
    assert p["buys"] == {"INSW.US": [(99.4, 20.0)], "AX.US": [(None, 5.0)]} and p["n_orders"] == 7   # done orders skipped
    assert ic.stop_cover(p, "DDI.US") == (14.8, 120.0) and ic.stop_cover(p, "GLPI.US") == (35.1, 119.0)   # the lowest stop
    assert ic.stop_cover(p, "IBOC.US") == (None, 0)
    assert ic.stop_cover({"stops": {"X.US": [(None, 50.0)]}}, "X.US") == (None, 50.0)             # covered, price unread
    assert ic.reserved_usd(p) == (pytest.approx(1988.0), False)                                   # the market buy: not known
    assert ic.reserved_usd(p, {"AX.US"}) == (pytest.approx(1988.0), True) and ic.reserved_usd(p, {"INSW.US", "AX.US"}) == (0.0, True)
    assert ic.symbol_of("ddi") == "DDI.US" and ic.symbol_of("2222.SR") == "2222.SR" and ic.symbol_of("") == ""
    base_only = capture()
    base_only["balances"]["balances"] = base_only["balances"]["balances"][:1]
    assert ic.parse(base_only, CREATED, NOW)["settled_usd"] == 4679.15                             # BASE when no USD row
    settled_only = capture()
    settled_only["balances"]["balances"] = [{"currency": "USD", "settled_cash": "1,200.50"}]
    assert ic.parse(settled_only, CREATED, NOW)["cash_usd"] == 1200.5


@pytest.mark.parametrize("change, reason", [
    (lambda c: c.update(schema="tfb.ibkr-capture/v2"), "capture schema not recognised"),
    (lambda c: c.update(captured_at="2026-11-13 08:41"), "capture time unreadable"),
    (lambda c: c.update(captured_at="2026-11-13T08:41:01"), "capture time unreadable"),                    # no offset
    (lambda c: c.update(captured_at="2026-11-13T12:06:00+03:00"), "capture time is in the future"),
    (lambda c: c.update(captured_at="2026-11-13T08:20:00+03:00"), "capture time disagrees with the file's creation time"),
    (lambda c: c.update(balances={"balances": []}), "capture has no USD balance"),
    (lambda c: c.update(balances={"balances": [{"currency": "USD"}]}), "capture balance has no cash"),
    (lambda c: c["balances"]["balances"].append({"currency": "EUR", "cash_balance": 12.0}),
     "capture holds cash in a currency other than USD"),
    (lambda c: c.update(positions={"error": "timeout"}), "capture has no positions list"),
    (lambda c: c.update(orders="n/a"), "capture has no orders list"),
])
def test_c2_a_capture_that_cannot_be_trusted_is_refused(change, reason):
    c = capture()
    change(c)
    p = ic.parse(c, CREATED, NOW)
    assert not p["ok"] and p["reason"] == reason and p["cash_usd"] in (None, 4679.15) and p["as_of"] is None
    assert not ic.parse(None, CREATED, NOW)["ok"] and not ic.parse([], CREATED, NOW)["ok"]


def test_c2_zulu_time_five_minutes_of_slack_and_a_zero_other_currency():
    c = capture(captured_at="2026-11-13T05:41:01Z")
    assert ic.parse(c, CREATED, NOW)["ok"] and ic.parse(c, None, NOW)["ok"]                          # no creation time: accepted
    assert ic.parse(capture(captured_at="2026-11-13T12:04:00+03:00"), None, NOW)["ok"]             # 4 minutes ahead
    eur0 = capture()
    eur0["balances"]["balances"].append({"currency": "EUR", "cash_balance": 0.0})
    assert ic.parse(eur0, CREATED, NOW)["ok"]


class Resp:
    def __init__(self, status=200, body=None, raw=None):
        self.status_code, self._body = status, body
        self.content = raw if raw is not None else json.dumps(body or {}).encode("utf-8")

    def json(self):
        return self._body


class Drive:
    def __init__(self, files=None, body=None, list_status=200, get_status=200, raw=None, boom=False):
        self.files, self.body, self.list_status, self.get_status, self.raw, self.boom = files, body, list_status, get_status, raw, boom
        self.calls = []

    def get(self, url, timeout=None, params=None):
        self.calls.append((url, dict(params or {})))
        if self.boom:
            raise RuntimeError("network")
        if url.endswith("/files"):
            return Resp(self.list_status, {"files": self.files or []})
        return Resp(self.get_status, self.body, self.raw)


FILES = [{"id": "new1", "name": "TFB_IBKR_Capture_20261113-084101.json", "createdTime": "2026-11-13T05:41:17.795Z", "size": "4537"},
         {"id": "old1", "name": "TFB_IBKR_Capture_20261112-200603.json", "createdTime": "2026-11-12T20:06:23.758Z", "size": "5395"}]
FOLDER = "1WbO2y3xMzPU0lKQ3gHJQ28L19IIAQdNH"


def test_c3_latest_reads_the_newest_file_and_never_raises(monkeypatch):
    said = []
    d = Drive(FILES, capture())
    p = ic.latest(NOW, session=d, folder=FOLDER, out=said.append)
    assert p["ok"] and p["name"] == FILES[0]["name"] and d.calls[1][0].endswith("/files/new1") and d.calls[1][1]["alt"] == "media"
    q = d.calls[0][1]["q"]
    assert FOLDER in q and "trashed = false" in q and "TFB_IBKR_Capture_" in q and d.calls[0][1]["orderBy"] == "createdTime desc"
    assert said == ["IBKR capture TFB_IBKR_Capture_20261113-084101.json read: 3.3 h old, 4 position(s), 7 working order(s)"]
    assert "4679" not in said[0] and "240" not in said[0]                                          # no amount, no quantity
    for kw, reason in ((dict(files=[]), "no capture file visible in the folder (is it shared with the service account?)"),
                       (dict(files=[{"id": "x", "name": "notes.json"}]), "no capture file visible in the folder (is it shared with the service account?)"),
                       (dict(files=FILES, list_status=403), "Drive list answered HTTP 403 (is the capture folder shared with the service account?)"),
                       (dict(files=FILES, get_status=404), "Drive download answered HTTP 404"),
                       (dict(files=FILES, raw=b"not json"), "capture is not JSON"),
                       (dict(files=[dict(FILES[0], size="900000")]), "capture size invalid"),
                       (dict(files=FILES, boom=True), "capture read failed (RuntimeError)")):
        r = ic.latest(NOW, session=Drive(**dict(dict(body=capture()), **kw)), folder=FOLDER, out=said.append)
        assert not r["ok"] and r["reason"] == reason, reason
    assert len(said) == 1                                                                         # only a good read is announced
    assert ic.latest(NOW, session=d, folder="")["reason"] == "capture folder not set (TFB_IBKR_CAPTURE_FOLDER_ID)"
    assert ic.latest(NOW, session=d, folder="bad id!")["reason"] == "capture folder id invalid"
    monkeypatch.setenv("TFB_IBKR_CAPTURE", "off")
    assert ic.latest(NOW, session=d, folder=FOLDER)["reason"] == "TFB_IBKR_CAPTURE is off"
    monkeypatch.delenv("TFB_IBKR_CAPTURE")
    monkeypatch.setenv("TFB_IBKR_CAPTURE_FOLDER_ID", FOLDER)
    assert ic.latest(NOW, session=Drive(FILES, capture()))["ok"]                                   # the folder from the env
    monkeypatch.delenv("GOOGLE_SHEETS_CREDENTIALS", raising=False)
    monkeypatch.delenv("GOOGLE_CREDENTIALS", raising=False)
    assert ic.latest(NOW)["reason"].startswith("capture read failed (")                           # no credentials: no raise
    src = (ROOT / "scripts/ibkr_capture.py").read_text(encoding="utf-8")
    assert "drive.readonly" in src and "auth/drive\"" not in src and ".post(" not in src and ".patch(" not in src and ".delete(" not in src
