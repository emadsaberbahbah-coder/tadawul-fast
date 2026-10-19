#!/usr/bin/env python3
# tests/test_security_pins_libsmoke_20261013.py
"""
Library behaviour smoke for the requirements.txt v1.1.5 security repairs.

VERSION 1.1.0 (2026-10-19)
WHY: verify the fixed FastAPI/Starlette pair against its exact production
pins. NLTK is removed because it is unused and GHSA-8mgp-746c-j5xp has no
patched release. L7 now requires its absence and exercises the production
news tokenizer; no financial or freshness gate changes are needed.

HISTORY: VERSION 1.0.0 (2026-10-13)
WHY: requirements.txt v1.1.4 moves twelve production pins (aiohttp, anyio,
cryptography, h2, lxml, nltk, protobuf, pydantic-settings, PyJWT, pyOpenSSL,
python-dotenv, python-multipart). No other test in tests/ imports any of
those twelve libraries directly and none drives httpx with http2=True, so
whole-suite parity guards the application logic, not the library behaviour
the code relies on. This harness is that missing guard, committed so the
owner can re-run it: each check exercises the same call shape the
production code uses, on whatever stack is installed. That version passed
on the old stack (v1.1.3) and on the new one (v1.1.4); L11 pins down the disclosed
behaviour change (h2 4.4 rejects a server SETTINGS ENABLE_PUSH=1).

No external network: L10/L11 use scripted in-memory HTTP/2 frames (no
socket at all), L12 talks to a 127.0.0.1 server on an ephemeral port, and
L15 imports main:app in a child process with every outbound connect and
getaddrinfo replaced by a raiser. Each check SKIPS when its library is not
installed (the lean CI lane has no production requirements); the heavy CI
lane installs requirements.txt and runs this file with
TFB_LIBSMOKE_REQUIRE_PINNED=1.

  L1  cryptography Fernet round trip (core/config.py's only cryptography use).
  L2  google-auth service-account signing with a throwaway RSA key (the Sheets
      auth path every workbook-writing workflow takes): sign, verify, and a
      google.auth.jwt encode/decode.
  L3  pyOpenSSL TLS client context (reserved pin; nothing imports it).
  L4  PyJWT HS256 encode/decode.
  L5  protobuf Struct round trip, plus imports of grpc, grpc_status,
      google.api_core, google.cloud.secretmanager, the OTLP gRPC exporter and
      googleapiclient (the protobuf 5.x consumers).
  L6  lxml parse, BeautifulSoup "lxml" and pandas.read_html(flavor="lxml") (the
      yfinance get_earnings_dates path used by calendar_provider).
  L7  NLTK is absent, its news capability flag is false, and the existing
      production tokenizer handles English and Arabic without it.
  L8  python-dotenv dotenv_values.
  L9  pydantic-settings env binding.
  L10 httpx.AsyncClient over an http2=True transport (the eodhd, finnhub and
      tadawul provider client shape) -> httpcore -> h2, against scripted
      server frames: 200, HTTP/2, a 40000-byte body in three DATA frames.
  L11 Same path, server SETTINGS carries ENABLE_PUSH=1. h2 >= 4.4 rejects it
      (RFC 9113 section 6.5.2; httpx raises LocalProtocolError "Setting 2 has
      invalid value 1"); h2 < 4.4 accepts it. Asserted per installed h2, so
      the known limit stays reproducible.
  L12 aiohttp.ClientSession POST json + GET of a chunked gzip JSON body
      against a 127.0.0.1 server (scripts/track_performance.py call shapes).
  L13 anyio task group.
  L14 python-multipart through FastAPI: urlencoded Form and a 70000-byte
      UploadFile via TestClient (in-memory ASGI).
  L15 import main:app in a child process with outbound sockets blocked:
      FastAPI app, openapi() builds, routes mounted with failed=0, zero
      blocked socket attempts.
  I1  the installed versions of the retained security packages and framework
      pair equal the pins in
      requirements.txt. When they differ (a stale local venv) it SKIPS,
      unless TFB_LIBSMOKE_REQUIRE_PINNED=1 (set in the heavy CI step), where
      it FAILS.
  G1  GOLDEN NEGATIVE: set TFB_LIBSMOKE_BASE_PYTHON=<python of a venv
      installed from the pre-change requirements.txt (v1.1.3)>; the installed
      versions read through that interpreter are checked against the
      security floors of tests/test_security_pins_20261013.py and this test
      FAILS (12 below their fixed versions). SKIPS when the variable is unset.

Run:  python tests/test_security_pins_libsmoke_20261013.py
      python -m pytest -q tests/test_security_pins_libsmoke_20261013.py
"""

import asyncio
import gzip
import importlib
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import traceback
import unittest
from pathlib import Path
from typing import Dict, List

HARNESS_VERSION = "1.1.0"

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "requirements.txt"
PINS_HARNESS = ROOT / "tests" / "test_security_pins_20261013.py"

# Retained v1.1.4 security pins plus the fixed v1.1.5 framework pair.
BUMPED = (
    "aiohttp", "anyio", "cryptography", "fastapi", "h2", "lxml", "protobuf",
    "pydantic-settings", "pyjwt", "pyopenssl", "python-dotenv",
    "python-multipart", "starlette",
)


# ---------------------------------------------------------------------------
# Helpers (import-time safe: nothing runs on import)
# ---------------------------------------------------------------------------

def _need(*modules: str) -> None:
    for name in modules:
        try:
            importlib.import_module(name)
        except Exception as exc:  # noqa: BLE001
            raise unittest.SkipTest("%s not importable here (%s: %s)" % (name, type(exc).__name__, exc))


def _version(dist: str) -> str:
    from importlib.metadata import version
    return version(dist)


def _vtuple(text: str):
    m = re.match(r"^\s*(\d+(?:\.\d+)*)", text or "")
    if not m:
        raise ValueError("not a release version: %r" % (text,))
    nums = [int(p) for p in m.group(1).split(".")]
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums)


def _pins_module():
    spec = importlib.util.spec_from_file_location("_tfb_secpins_20261013", PINS_HARNESS)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


_VERSIONS_SNIPPET = (
    "import json\n"
    "from importlib.metadata import version, PackageNotFoundError\n"
    "out = {}\n"
    "for d in %r:\n"
    "    try:\n"
    "        out[d] = version(d)\n"
    "    except PackageNotFoundError:\n"
    "        out[d] = ''\n"
    "print(json.dumps(out, sort_keys=True))\n"
)


def _installed_versions(python: str) -> Dict[str, str]:
    proc = subprocess.run(
        [python, "-I", "-c", _VERSIONS_SNIPPET % (BUMPED,)],
        capture_output=True, text=True, timeout=60, check=False,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


def _h2_frames(enable_push: bool, body: bytes) -> List[bytes]:
    """Scripted server side of one HTTP/2 exchange on stream 1."""
    import hpack
    import hyperframe.frame as hf

    settings = {hf.SettingsFrame.ENABLE_PUSH: 1} if enable_push else {}
    frames = [hf.SettingsFrame(settings=settings).serialize()]
    block = hpack.Encoder().encode([
        (b":status", b"200"),
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode()),
    ])
    frames.append(hf.HeadersFrame(stream_id=1, data=block, flags=["END_HEADERS"]).serialize())
    step = 16384
    for i in range(0, len(body), step):
        last = i + step >= len(body)
        flags = ["END_STREAM"] if last else []
        frames.append(hf.DataFrame(stream_id=1, data=body[i:i + step], flags=flags).serialize())
    return frames


def _h2_get(enable_push: bool, body: bytes):
    """httpx.AsyncClient over an http2=True transport fed by scripted frames."""
    import httpcore
    import httpx

    transport = httpx.AsyncHTTPTransport(http2=True)
    if not hasattr(transport, "_pool"):
        raise unittest.SkipTest("httpx transport has no _pool to back with a mock network")
    transport._pool = httpcore.AsyncConnectionPool(
        http2=True,
        network_backend=httpcore.AsyncMockBackend(_h2_frames(enable_push, body), http2=True),
    )

    async def go():
        async with httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(10.0)) as client:
            return await client.get("https://provider.invalid/api/real-time/TEST")

    return asyncio.run(go())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_l01_cryptography_fernet_round_trip() -> None:
    _need("cryptography.fernet")
    from cryptography.fernet import Fernet
    f = Fernet(Fernet.generate_key())
    assert f.decrypt(f.encrypt(b"tfb")) == b"tfb"
    print("L1 cryptography %s Fernet ok" % _version("cryptography"))


def test_l02_google_auth_service_account_signing() -> None:
    _need("cryptography", "google.auth", "google.oauth2.service_account")
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from google.auth import crypt
    from google.auth import jwt as gjwt
    from google.oauth2 import service_account

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    pub = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    info = {
        "type": "service_account", "project_id": "p", "private_key_id": "k1",
        "private_key": pem, "client_email": "sa@p.iam.gserviceaccount.com",
        "client_id": "1", "token_uri": "https://oauth2.googleapis.com/token",
    }
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    sig = creds.signer.sign(b"tfb")
    assert crypt.RSAVerifier.from_string(pub).verify(b"tfb", sig)
    now = int(time.time())
    tok = gjwt.encode(creds.signer, {"iss": info["client_email"], "aud": "x", "iat": now, "exp": now + 60})
    claims = gjwt.decode(tok, certs={"k1": pub}, audience="x")
    assert claims["iss"] == info["client_email"]
    print("L2 google-auth %s cryptography %s signing ok" % (_version("google-auth"), _version("cryptography")))


def test_l03_pyopenssl_tls_client_context() -> None:
    _need("OpenSSL.SSL")
    from OpenSSL import SSL
    ctx = SSL.Context(SSL.TLS_CLIENT_METHOD)
    assert ctx is not None
    print("L3 pyOpenSSL %s context ok" % _version("pyopenssl"))


def test_l04_pyjwt_hs256_round_trip() -> None:
    _need("jwt")
    import jwt
    secret = "k" * 32
    tok = jwt.encode({"sub": "tfb"}, secret, algorithm="HS256")
    assert jwt.decode(tok, secret, algorithms=["HS256"])["sub"] == "tfb"
    print("L4 PyJWT %s HS256 ok" % _version("pyjwt"))


def test_l05_protobuf_and_its_consumers() -> None:
    _need("google.protobuf.struct_pb2")
    from google.protobuf import json_format, struct_pb2
    s = struct_pb2.Struct()
    s.update({"a": 1.5, "b": "x"})
    s2 = struct_pb2.Struct()
    s2.ParseFromString(s.SerializeToString())
    assert json_format.MessageToDict(s2) == {"a": 1.5, "b": "x"}
    consumers = (
        "grpc", "grpc_status.rpc_status", "google.api_core.exceptions",
        "google.cloud.secretmanager",
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter",
        "googleapiclient.discovery",
    )
    strict = (os.environ.get("TFB_LIBSMOKE_REQUIRE_PINNED", "") or "").strip() == "1"
    imported = []
    for name in consumers:
        try:
            importlib.import_module(name)
        except ImportError as exc:
            if strict or "protobuf" in str(exc).lower():
                raise
            continue
        imported.append(name)
    print("L5 protobuf %s ok; consumers imported %d/%d" % (_version("protobuf"), len(imported), len(consumers)))


def test_l06_lxml_bs4_and_pandas_read_html() -> None:
    _need("lxml.etree")
    from lxml import etree
    assert etree.fromstring(b"<a><b>1</b></a>").findtext("b") == "1"
    checked = ["lxml"]
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        BeautifulSoup = None
    if BeautifulSoup is not None:
        assert BeautifulSoup("<p><i>x</i></p>", "lxml").i.text == "x"
        checked.append("bs4")
    try:
        import pandas as pd
    except ImportError:
        pd = None
    if pd is not None:
        html = "<table><tr><th>Earnings Date</th><th>EPS</th></tr><tr><td>2026-10-20</td><td>-</td></tr></table>"
        df = pd.read_html(io.StringIO(html), flavor="lxml", na_values=["-"])[0]
        assert list(df.columns) == ["Earnings Date", "EPS"], list(df.columns)
        assert df["Earnings Date"][0] == "2026-10-20"
        assert bool(df["EPS"].isna()[0])
        checked.append("pandas.read_html")
    print("L6 lxml %s ok: %s" % (_version("lxml"), ", ".join(checked)))


def test_l07_news_tokenizer_does_not_require_nltk() -> None:
    assert importlib.util.find_spec("nltk") is None, "unused vulnerable NLTK must not be installed"
    from core import news_intelligence as news
    assert news.HAS_NLTK is False
    assert news._tokenize("Hi, there") == ["hi", "there"]
    assert news._tokenize("Growth أرباح") == ["growth", "أرباح"]
    print("L7 NLTK absent; production English/Arabic tokenizer ok")


def test_l08_python_dotenv_values() -> None:
    _need("dotenv")
    from dotenv import dotenv_values
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, ".env")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("A=1\nB='two words'\n# comment\nC=\"x=y\"\n")
        assert dotenv_values(path) == {"A": "1", "B": "two words", "C": "x=y"}
    print("L8 python-dotenv %s ok" % _version("python-dotenv"))


def test_l09_pydantic_settings_env_binding() -> None:
    _need("pydantic_settings")
    from pydantic_settings import BaseSettings, SettingsConfigDict

    class _S(BaseSettings):
        model_config = SettingsConfigDict(env_prefix="TFBLIBSMOKE_")
        port: int = 1

    key = "TFBLIBSMOKE_PORT"
    old = os.environ.get(key)
    os.environ[key] = "8080"
    try:
        assert _S().port == 8080
    finally:
        if old is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old
    print("L9 pydantic-settings %s ok" % _version("pydantic-settings"))


def test_l10_httpx_http2_round_trip_through_h2() -> None:
    _need("httpx", "httpcore", "h2", "hpack", "hyperframe")
    body = b'{"close":' + b"1" * 39990 + b"}"
    assert len(body) == 40000
    resp = _h2_get(enable_push=False, body=body)
    assert resp.status_code == 200, resp.status_code
    assert resp.http_version == "HTTP/2", resp.http_version
    assert resp.content == body
    print("L10 h2 %s httpx %s httpcore %s: 200 HTTP/2 %d bytes" % (
        _version("h2"), _version("httpx"), _version("httpcore"), len(resp.content)))


def test_l11_h2_server_enable_push_behaviour_matches_installed_h2() -> None:
    _need("httpx", "httpcore", "h2", "hpack", "hyperframe")
    import httpx
    h2v = _version("h2")
    rejects = _vtuple(h2v) >= (4, 4, 0)
    try:
        resp = _h2_get(enable_push=True, body=b"{}")
        outcome = "accepted %d" % resp.status_code
        accepted = resp.status_code == 200
    except httpx.TransportError as exc:
        outcome = "%s: %s" % (type(exc).__name__, exc)
        accepted = False
        if rejects:
            assert isinstance(exc, httpx.LocalProtocolError), outcome
            assert "Setting 2 has invalid value 1" in str(exc), outcome
    print("L11 h2 %s server ENABLE_PUSH=1 -> %s" % (h2v, outcome))
    if rejects:
        assert not accepted, "h2 %s was expected to reject ENABLE_PUSH=1" % h2v
    else:
        assert accepted, "h2 %s was expected to accept ENABLE_PUSH=1: %s" % (h2v, outcome)


def test_l12_aiohttp_client_against_loopback() -> None:
    _need("aiohttp")
    import aiohttp

    chart = {"chart": {"result": [{"close": [1.5, 2.5]}]}}

    async def handle(reader, writer):
        head = await reader.readuntil(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        method = lines[0].split(" ", 1)[0]
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        n = int(headers.get("content-length", "0") or 0)
        body = await reader.readexactly(n) if n else b""
        if method == "POST":
            payload = json.dumps({"echo": json.loads(body.decode())}).encode()
            out = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                   b"Content-Length: %d\r\nConnection: close\r\n\r\n" % len(payload)) + payload
        else:
            raw = gzip.compress(json.dumps(chart).encode())
            out = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                   b"Content-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n"
                   b"Connection: close\r\n\r\n")
            for part in (raw[:10], raw[10:]):
                out += b"%x\r\n" % len(part) + part + b"\r\n"
            out += b"0\r\n\r\n"
        writer.write(out)
        await writer.drain()
        writer.close()

    async def go():
        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        base = "http://127.0.0.1:%d" % port
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "tfb-libsmoke"}) as session:
                async with session.post(base + "/v1/batch", json={"symbols": ["1120.SR"]}) as resp:
                    code = int(resp.status)
                    raw = await resp.read()
                async with session.get(base + "/v8/finance/chart/AAPL") as resp2:
                    code2 = int(resp2.status)
                    data2 = await resp2.json()
        finally:
            server.close()
            await server.wait_closed()
        return code, json.loads(raw), code2, data2

    code, data, code2, data2 = asyncio.run(go())
    assert code == 200 and data == {"echo": {"symbols": ["1120.SR"]}}, (code, data)
    assert code2 == 200 and data2 == chart, (code2, data2)
    print("L12 aiohttp %s POST json + GET chunked gzip ok" % _version("aiohttp"))


def test_l13_anyio_task_group() -> None:
    _need("anyio")
    import anyio
    seen = []

    async def worker(i):
        await anyio.sleep(0)
        seen.append(i)

    async def go():
        async with anyio.create_task_group() as tg:
            for i in range(3):
                tg.start_soon(worker, i)
        return sorted(seen)

    assert anyio.run(go) == [0, 1, 2]
    print("L13 anyio %s ok" % _version("anyio"))


def test_l14_python_multipart_through_fastapi() -> None:
    _need("fastapi", "fastapi.testclient")
    try:
        importlib.import_module("python_multipart")  # 0.0.13 and later
    except ImportError:
        _need("multipart")  # 0.0.12 ships only the legacy module name
    from fastapi import FastAPI, File, Form, UploadFile
    from fastapi.testclient import TestClient

    app = FastAPI()

    @app.post("/f")
    def f(a: str = Form(...), b: int = Form(...)):
        return {"a": a, "b": b}

    @app.post("/u")
    async def u(name: str = Form(...), up: UploadFile = File(...)):
        data = await up.read()
        return {"name": name, "fn": up.filename, "n": len(data)}

    client = TestClient(app)
    r1 = client.post("/f", data={"a": "x y", "b": "7"})
    r2 = client.post("/u", data={"name": "z"},
                     files={"up": ("t.bin", b"\x00" * 70000, "application/octet-stream")})
    assert r1.status_code == 200 and r1.json() == {"a": "x y", "b": 7}, (r1.status_code, r1.text)
    assert r2.status_code == 200 and r2.json() == {"name": "z", "fn": "t.bin", "n": 70000}, (r2.status_code, r2.text)
    print("L14 python-multipart %s starlette %s form+upload ok" % (
        _version("python-multipart"), _version("starlette")))


_IMPORT_MAIN = r'''
import json, os, socket, sys
blocked = []
def _deny(*a, **k):
    blocked.append(repr(a[:2])[:120])
    raise OSError("network disabled by libsmoke L15")
socket.socket.connect = _deny
socket.socket.connect_ex = _deny
socket.create_connection = _deny
socket.getaddrinfo = _deny
sys.path.insert(0, os.getcwd())
import main
from fastapi import FastAPI
app = main.app
assert isinstance(app, FastAPI), type(app)
schema = app.openapi()
print("L15JSON " + json.dumps({"app_type": type(app).__name__, "routes": len(app.routes),
    "openapi_paths": len(schema.get("paths", {})), "blocked": len(blocked)}, sort_keys=True))
'''


def test_l15_import_main_app_with_sockets_blocked() -> None:
    _need("fastapi", "pydantic_settings", "httpx")
    if not (ROOT / "main.py").is_file():
        raise unittest.SkipTest("main.py not found")
    with tempfile.TemporaryDirectory() as home:
        env = {"PATH": os.environ.get("PATH", os.defpath), "HOME": home,
               "LANG": "C.UTF-8", "PYTHONDONTWRITEBYTECODE": "1"}
        proc = subprocess.run(
            [sys.executable, "-c", _IMPORT_MAIN], cwd=str(ROOT), env=env,
            capture_output=True, text=True, timeout=300, check=False,
        )
    combined = proc.stdout + "\n" + proc.stderr
    assert proc.returncode == 0, combined[-4000:]
    line = [ln for ln in proc.stdout.splitlines() if ln.startswith("L15JSON ")]
    assert line, combined[-4000:]
    info = json.loads(line[-1][len("L15JSON "):])
    assert info["app_type"] == "FastAPI", info
    assert info["blocked"] == 0, info
    assert info["openapi_paths"] > 0 and info["routes"] > 0, info
    mounted = re.search(r"Routes mounted at app creation: (.*)", combined)
    if mounted:
        assert "failed=0" in mounted.group(1) and "missing=0" in mounted.group(1), mounted.group(0)
    print("L15 main:app %s; %s" % (json.dumps(info, sort_keys=True),
                                   mounted.group(0) if mounted else "no mount line"))


def test_i1_installed_bumped_versions_equal_requirements() -> None:
    pins = _pins_module()
    reqs = pins._parse_requirements(REQUIREMENTS)
    want = {name: pins._exact(reqs, name) for name in BUMPED}
    have = _installed_versions(sys.executable)
    missing = sorted(n for n in BUMPED if not have.get(n))
    differ = ["%s installed %s, requirements.txt %s" % (n, have[n], want[n])
              for n in BUMPED if have.get(n) and have[n] != want[n]]
    strict = (os.environ.get("TFB_LIBSMOKE_REQUIRE_PINNED", "") or "").strip() == "1"
    print("I1 python=%s missing=%d differ=%d strict=%s" % (sys.executable, len(missing), len(differ), strict))
    for line in differ:
        print("I1   " + line)
    if not strict and (missing or differ):
        raise unittest.SkipTest(
            "installed stack is not requirements.txt (missing=%d differ=%d); "
            "set TFB_LIBSMOKE_REQUIRE_PINNED=1 to make this a failure" % (len(missing), len(differ)))
    assert not missing and not differ, "missing=%r\n%s" % (missing, "\n".join(differ))


def test_g1_golden_negative_base_stack_fails_the_floors() -> None:
    base = (os.environ.get("TFB_LIBSMOKE_BASE_PYTHON", "") or "").strip()
    if not base:
        raise unittest.SkipTest(
            "golden-negative skipped: set TFB_LIBSMOKE_BASE_PYTHON=<python of a venv "
            "installed from requirements.txt v1.1.3>")
    if not Path(base).exists():
        raise unittest.SkipTest("TFB_LIBSMOKE_BASE_PYTHON does not exist: " + base)
    floors = _pins_module().SECURITY_FLOORS
    have = _installed_versions(base)
    bad = []
    for name in BUMPED:
        ver = have.get(name, "")
        if not ver:
            bad.append("%s not installed" % name)
        elif _vtuple(ver) < _vtuple(floors[name]):
            bad.append("%s %s installed, below its fixed version %s" % (name, ver, floors[name]))
    print("G1 base=%s violations=%d" % (base, len(bad)))
    for line in bad:
        print("G1   " + line)
    assert not bad, "base stack fails the security floors:\n" + "\n".join(bad)


# ---------------------------------------------------------------------------
# Standalone runner (pytest ignores this)
# ---------------------------------------------------------------------------

def _main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    passed = failed = skipped = 0
    for name, fn in tests:
        try:
            fn()
        except unittest.SkipTest as exc:
            skipped += 1
            print("SKIP " + name + " :: " + str(exc))
            continue
        except Exception:  # noqa: BLE001
            failed += 1
            print("FAIL " + name)
            traceback.print_exc()
            continue
        passed += 1
        print("ok   " + name)
    total = passed + failed
    suffix = ("  (skipped %d)" % skipped) if skipped else ""
    print("PASS %d/%d%s" % (passed, total, suffix))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
