#!/usr/bin/env python3
"""scripts/render_env_contract.py - generate docs/render_env_contract.md.

VERSION 1.0.0 (2026-10-13) - NEW SCRIPT (Build Spec item H-8)

WHY (2026-10-13): H-8 asks for a Render ENV contract and the repository had
none. The gap was visible on the live service: on 13 Oct 2026 the owner's
/health showed app_version 5.111.0 while entry_version was 8.14.3.
app_version is not a code constant. config.py reads TFB_APP_VERSION, then
APP_VERSION, and main.py falls back to its APP_ENTRY_VERSION only when both
are unset, so one of those two keys is still set in the Render service ENV or
in a linked environment group. A hand-written contract would go stale at
once (docs/flags.md already trails HEAD), so this file generates the contract
from the code it describes.

WHAT IT WRITES (docs/render_env_contract.md):
  A. platform keys outside the TFB_ prefix: TFB_APP_VERSION and APP_VERSION
     (MUST be unset), the other version aliases, APP_ENV and the names read
     before it, PORT, WEB_CONCURRENCY, REDIS_URL and the RENDER_* deploy keys
     main.py reports in the /health "deploy" block;
  B. the gates /health shows: main.py _PF_GATE_ENVS and _OB_GATE_ENVS (the
     "pf_gates" block) and the "engine_gates" block that
     core/data_engine_v2.py surface_gate_states() builds, each with its code
     default and, where a static evaluation can tell, the value when unset;
  C. the Render-scoped entries of scripts/verify_deployment.py FLAGS (entries
     in its _WORKFLOW_SCOPED set are left out);
  D. a pointer to docs/flags.md (not a copy of it).

RULES:
  - Stdlib and ast only. It NEVER imports the modules it scans: every source
    file is read as text and parsed. Defaults come from a small static
    evaluator that substitutes "unset" for every ENV read; anything it cannot
    evaluate is shown as "-", never guessed.
  - It never reads os.environ, so the document cannot depend on, or leak, the
    environment of the machine that runs it.
  - Names and code defaults only. Owner decision D7 (public repository) is
    open, so key names for authentication, tokens and other secrets are left
    out of the document (they are counted, not named).
  - Expected production values come only from the owner-filled
    config/render_env_expected.json. When it is absent the document says
    "expected production value: not recorded". Values that do not look like a
    plain flag value are withheld, never printed.

USAGE:
  python scripts/render_env_contract.py              # write the doc; WARN-only
  python scripts/render_env_contract.py --check      # exit 1 if the doc is stale
  python scripts/render_env_contract.py --output -   # print to stdout
  Options: --output PATH (default docs/render_env_contract.md under --root),
           --root DIR (default: this repository),
           --expected PATH (default config/render_env_expected.json under --root).

EXIT CODES:
  default  0 (prints WARN when the written content differs from what was on
           disk); 2 on a usage or input error (for example an unreadable
           expected-values file).
  --check  0 when the file equals the generator output, 1 when it differs or
           is missing; nothing is written.
"""
from __future__ import annotations

import argparse
import ast
import builtins
import json
import re
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

SCRIPT_VERSION = "1.0.0"

DEFAULT_OUTPUT = "docs/render_env_contract.md"
DEFAULT_EXPECTED = "config/render_env_expected.json"
REGENERATE_COMMAND = "python scripts/render_env_contract.py"

MAIN_FILE = "main.py"
ENGINE_FILE = "core/data_engine_v2.py"
PF_FILE = "core/analysis/portfolio_actions.py"
OB_FILE = "core/analysis/opportunity_builder.py"
VERIFIER_FILE = "scripts/verify_deployment.py"
FLAGS_DOC = "docs/flags.md"
ENV_GROUP = "tfb-server"

# Files checked for "read by" in section A. Fixed on purpose: a doc whose
# content moved with every new reader anywhere would churn on unrelated edits.
PLATFORM_READ_FILES = (
    "main.py", "config.py", "env.py", "core/config.py",
    "core/data_engine_v2.py", "core/analysis/portfolio_actions.py",
    "scripts/worker.py", "scripts/start_web.sh",
)

ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+$|^[A-Z]{3,}$")
# Owner decision D7: names that carry or switch authentication, tokens or
# other secrets stay out of the public document.
WITHHELD_NAME_RE = re.compile(
    r"AUTH|TOKEN|SECRET|PASSWORD|PASSWD|PASSPHRASE|API_?KEY|PRIVATE|CREDENTIAL|"
    r"BEARER|OPEN_MODE|SIGNING|COOKIE|SESSION_KEY|_KEYS?$|^KEY_")
PLAIN_VALUE_RE = re.compile(r"^[A-Za-z0-9_.,:;+\-/ %=]{0,64}$")
SECRETISH_VALUE_RE = re.compile(r"^[A-Za-z0-9+/=_\-]{24,}$")

MUST_UNSET = "MUST be unset"
NOT_RECORDED = "not recorded"


class GeneratorError(Exception):
    """An input problem the operator must fix (exit 2)."""


# --------------------------------------------------------------------------
# ASCII and Markdown helpers
# --------------------------------------------------------------------------
_ASCII_MAP = {
    "\u2014": "-", "\u2013": "-", "\u2212": "-", "\u2018": "'", "\u2019": "'",
    "\u201c": '"', "\u201d": '"', "\u00d7": "x", "\u2192": "->", "\u2190": "<-",
    "\u2026": "...", "\u00a0": " ", "\u2265": ">=", "\u2264": "<=",
}


def ascii_text(text: Any) -> str:
    """Deterministic ASCII rendering of any text the doc quotes from code."""
    out = []
    for ch in str(text):
        if ord(ch) < 128:
            out.append(ch)
            continue
        if ch in _ASCII_MAP:
            out.append(_ASCII_MAP[ch])
            continue
        folded = unicodedata.normalize("NFKD", ch).encode("ascii", "ignore").decode("ascii")
        out.append(folded or "?")
    return "".join(out)


def cell(text: Any) -> str:
    """A Markdown table cell: ASCII, single line, pipes escaped."""
    s = ascii_text(text).replace("\r", " ").replace("\n", " ").strip()
    return s.replace("|", "&#124;")


def code(text: Any) -> str:
    return "`" + cell(text).replace("`", "'") + "`"


def show(value: Any) -> str:
    """repr() of a statically evaluated value, ASCII."""
    return ascii_text(repr(value))


def is_withheld_name(name: str) -> bool:
    return bool(WITHHELD_NAME_RE.search(str(name).upper()))


# --------------------------------------------------------------------------
# Static source model (parsed, never imported)
# --------------------------------------------------------------------------
class SourceModule:
    """One Python file, parsed with ast. Never imported, never executed."""

    def __init__(self, repo: "Repo", rel: str, text: str) -> None:
        self.repo = repo
        self.rel = rel
        self.tree = ast.parse(text, filename=rel)
        self.parents: Dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(self.tree):
            for child in ast.iter_child_nodes(parent):
                self.parents[child] = parent
        self.consts: Dict[str, Any] = {}
        self.lazy: Dict[str, ast.AST] = {}
        self.funcs: Dict[str, ast.FunctionDef] = {}
        self.imports: Dict[str, Tuple[str, str]] = {}
        self.os_names = {"os"}
        self.getenv_names = set()
        self.environ_names = set()
        self._reader_nodes: Optional[List[ast.AST]] = None
        for node in self.tree.body:
            self._index_top_level(node)
        self.env_funcs = self._env_funcs()
        self.helper_readers = self._helper_readers()

    # -- indexing -----------------------------------------------------------
    def _index_top_level(self, node: ast.AST) -> None:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "os":
                    self.os_names.add(alias.asname or "os")
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            for alias in node.names:
                local = alias.asname or alias.name
                if node.module == "os":
                    if alias.name == "getenv":
                        self.getenv_names.add(local)
                    elif alias.name == "environ":
                        self.environ_names.add(local)
                    continue
                self.imports[local] = (node.module, alias.name)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            self.funcs[node.name] = node
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            value = node.value
            if value is None:
                return
            for target in targets:
                if not isinstance(target, ast.Name):
                    continue
                if isinstance(value, ast.Name) and value.id in self.funcs:
                    self.funcs[target.id] = self.funcs[value.id]
                    continue
                try:
                    self.consts[target.id] = ast.literal_eval(value)
                    self.lazy.pop(target.id, None)
                except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
                    # Not a literal (e.g. a dict naming another constant):
                    # evaluated on demand by the Evaluator, never executed.
                    self.consts.pop(target.id, None)
                    self.lazy[target.id] = value
        elif isinstance(node, ast.Try):
            # Optional imports at module level (try: from x import y).
            for inner in node.body:
                if isinstance(inner, (ast.Import, ast.ImportFrom)):
                    self._index_top_level(inner)

    def is_direct_reader(self, func: ast.AST) -> bool:
        if isinstance(func, ast.Name):
            return func.id in self.getenv_names
        if not isinstance(func, ast.Attribute):
            return False
        if func.attr == "getenv" and isinstance(func.value, ast.Name):
            return func.value.id in self.os_names
        if func.attr == "get":
            return self.is_environ(func.value)
        return False

    def is_environ(self, node: ast.AST) -> bool:
        if isinstance(node, ast.Name):
            return node.id in self.environ_names
        return (isinstance(node, ast.Attribute) and node.attr == "environ"
                and isinstance(node.value, ast.Name) and node.value.id in self.os_names)

    def _env_funcs(self) -> set:
        """Module functions that read ENV directly or through another one."""
        found = set()
        for name, fn in self.funcs.items():
            if any(isinstance(n, ast.Call) and self.is_direct_reader(n.func) for n in ast.walk(fn)):
                found.add(name)
        changed = True
        while changed:
            before = len(found)
            for name, fn in self.funcs.items():
                if name in found:
                    continue
                if any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                       and n.func.id in found for n in ast.walk(fn)):
                    found.add(name)
            changed = len(found) != before
        return found

    def _helper_readers(self) -> Dict[str, Tuple[int, Optional[str]]]:
        """Functions whose FIRST parameter is the ENV name they read:
        name -> (index of the default argument or -1, default keyword)."""
        helpers: Dict[str, Tuple[int, Optional[str]]] = {}
        changed = True
        while changed:
            before = len(helpers)
            for name, fn in self.funcs.items():
                if name in helpers:
                    continue
                params = [a.arg for a in fn.args.posonlyargs + fn.args.args]
                if not params:
                    continue
                first = params[0]
                for node in ast.walk(fn):
                    if not (isinstance(node, ast.Call) and node.args):
                        continue
                    arg0 = node.args[0]
                    if not (isinstance(arg0, ast.Name) and arg0.id == first):
                        continue
                    if self.is_direct_reader(node.func) or (
                            isinstance(node.func, ast.Name) and node.func.id in helpers):
                        default_kw = "default" if "default" in params else None
                        default_idx = 1 if len(params) > 1 else -1
                        helpers[name] = (default_idx, default_kw)
                        break
            changed = len(helpers) != before
        return helpers

    # -- static read sites ----------------------------------------------------
    def const_str(self, node: ast.AST) -> Optional[str]:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Name) and isinstance(self.consts.get(node.id), str):
            return self.consts[node.id]
        return None

    def enclosing_function(self, node: ast.AST) -> Optional[ast.FunctionDef]:
        cur = self.parents.get(node)
        while cur is not None:
            if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return cur
            cur = self.parents.get(cur)
        return None

    def reader_nodes(self) -> List[ast.AST]:
        """Every node that can read ENV, found in one walk and cached."""
        cached = self._reader_nodes
        if cached is None:
            cached = []
            for node in ast.walk(self.tree):
                if (isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Load)
                        and self.is_environ(node.value)):
                    cached.append(node)
                elif isinstance(node, ast.Call) and (
                        self.is_direct_reader(node.func)
                        or (isinstance(node.func, ast.Name) and node.func.id in self.env_funcs)):
                    cached.append(node)
            self._reader_nodes = cached
        return cached

    def read_sites(self, env_name: str) -> List[Dict[str, Any]]:
        """Every call that reads env_name, with its raw code default."""
        sites = []
        for node in self.reader_nodes():
            if isinstance(node, ast.Subscript) and self.is_environ(node.value):
                key = node.slice
                if self.const_str(key) == env_name:
                    sites.append({"node": node, "default": "KeyError (required)"})
                continue
            if not isinstance(node, ast.Call):
                continue
            default_node: Optional[ast.AST] = None
            has_default = False
            if self.is_direct_reader(node.func):
                if not node.args or self.const_str(node.args[0]) != env_name:
                    continue
                if len(node.args) > 1:
                    default_node, has_default = node.args[1], True
            elif isinstance(node.func, ast.Name) and node.func.id in self.helper_readers:
                if not node.args or self.const_str(node.args[0]) != env_name:
                    continue
                idx, kw = self.helper_readers[node.func.id]
                for k in node.keywords:
                    if kw and k.arg == kw:
                        default_node, has_default = k.value, True
                if not has_default and idx >= 1 and len(node.args) > idx:
                    default_node, has_default = node.args[idx], True
                if not has_default:
                    fn = self.funcs[node.func.id]
                    defaults = fn.args.defaults
                    params = fn.args.posonlyargs + fn.args.args
                    if idx >= 1 and len(params) > idx and len(defaults) >= len(params) - idx:
                        default_node = defaults[idx - (len(params) - len(defaults))]
                        has_default = True
            elif isinstance(node.func, ast.Name) and node.func.id in self.env_funcs:
                names = [self.const_str(a) for a in node.args]
                if env_name not in names:
                    continue
                for k in node.keywords:
                    if k.arg == "default":
                        default_node, has_default = k.value, True
            else:
                continue
            parent = self.parents.get(node)
            if (not has_default and isinstance(parent, ast.BoolOp)
                    and isinstance(parent.op, ast.Or) and parent.values[0] is node
                    and len(parent.values) > 1):
                default_node, has_default = parent.values[1], True
            if has_default:
                try:
                    raw = show(Evaluator(self.repo).expr(default_node, {}, self))
                except Exception:  # noqa: BLE001 - any failure is "computed"
                    raw = "computed"
            else:
                raw = "None"
            sites.append({"node": node, "default": raw})
        return sites


class Repo:
    """A source tree read as text. Parsed modules are cached per path."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self._cache: Dict[str, Optional[SourceModule]] = {}

    def text(self, rel: str) -> Optional[str]:
        path = self.root / rel
        if not path.is_file():
            return None
        return path.read_text(encoding="utf-8", errors="replace")

    def module(self, rel: str) -> Optional[SourceModule]:
        if rel not in self._cache:
            text = self.text(rel)
            self._cache[rel] = SourceModule(self, rel, text) if text is not None else None
        return self._cache[rel]

    def require(self, rel: str) -> SourceModule:
        mod = self.module(rel)
        if mod is None:
            raise GeneratorError(f"required source file is missing: {rel}")
        return mod

    def module_for_import(self, dotted: str) -> Optional[SourceModule]:
        rel = dotted.replace(".", "/") + ".py"
        return self.module(rel)


# --------------------------------------------------------------------------
# Static evaluator: "what does this code return when every ENV is unset?"
# --------------------------------------------------------------------------
class Undetermined(Exception):
    """The static evaluator cannot tell; the doc then shows '-'."""


class _Return(Exception):
    def __init__(self, value: Any) -> None:
        super().__init__()
        self.value = value


_SAFE_BUILTINS = {
    "str": str, "int": int, "float": float, "bool": bool, "abs": abs,
    "len": len, "any": any, "all": all, "min": min, "max": max,
    "round": round, "tuple": tuple, "list": list, "set": set,
    "frozenset": frozenset, "sorted": sorted,
}
_STR_METHODS = {"strip", "lstrip", "rstrip", "lower", "upper", "casefold",
                "replace", "split", "startswith", "endswith"}
_SCALARS = (str, int, float, bool, type(None))
_BINOPS = {ast.Add: lambda a, b: a + b, ast.Sub: lambda a, b: a - b,
           ast.Mult: lambda a, b: a * b, ast.Div: lambda a, b: a / b}
_CMPOPS = {ast.Eq: lambda a, b: a == b, ast.NotEq: lambda a, b: a != b,
           ast.Lt: lambda a, b: a < b, ast.LtE: lambda a, b: a <= b,
           ast.Gt: lambda a, b: a > b, ast.GtE: lambda a, b: a >= b,
           ast.In: lambda a, b: a in b, ast.NotIn: lambda a, b: a not in b,
           ast.Is: lambda a, b: a is b, ast.IsNot: lambda a, b: a is not b}


class Evaluator:
    """Evaluates a small, side-effect-free subset of Python over the AST.

    Every ENV read returns its unset value (the default argument, or None);
    os.environ behaves as an empty mapping. Reads are recorded. Anything
    outside the subset raises Undetermined - the caller prints '-'."""

    MAX_DEPTH = 8
    MAX_STEPS = 20000

    def __init__(self, repo: Repo) -> None:
        self.repo = repo
        self.reads: List[List[str]] = []
        self.steps = 0
        self.depth = 0
        self._busy: set = set()

    # -- functions ------------------------------------------------------------
    def call_function(self, mod: SourceModule, fn: ast.FunctionDef,
                      args: Sequence[Any] = (), kwargs: Optional[Dict[str, Any]] = None) -> Any:
        if isinstance(fn, ast.AsyncFunctionDef):
            raise Undetermined("async")
        if self.depth >= self.MAX_DEPTH:
            raise Undetermined("depth")
        kwargs = dict(kwargs or {})
        spec = fn.args
        params = spec.posonlyargs + spec.args
        local: Dict[str, Any] = {}
        for i, param in enumerate(params):
            if i < len(args):
                local[param.arg] = args[i]
            elif param.arg in kwargs:
                local[param.arg] = kwargs.pop(param.arg)
            else:
                d_index = i - (len(params) - len(spec.defaults))
                if d_index < 0:
                    raise Undetermined("missing argument " + param.arg)
                local[param.arg] = self.expr(spec.defaults[d_index], {}, mod)
        extra = list(args[len(params):])
        if extra and not spec.vararg:
            raise Undetermined("too many arguments")
        if spec.vararg:
            local[spec.vararg.arg] = tuple(extra)
        for param, default in zip(spec.kwonlyargs, spec.kw_defaults):
            if param.arg in kwargs:
                local[param.arg] = kwargs.pop(param.arg)
            elif default is not None:
                local[param.arg] = self.expr(default, {}, mod)
            else:
                raise Undetermined("missing keyword " + param.arg)
        if kwargs:
            raise Undetermined("unexpected keywords")
        self.depth += 1
        try:
            self.block(fn.body, local, mod)
        except _Return as ret:
            return ret.value
        finally:
            self.depth -= 1
        return None

    def block(self, stmts: Sequence[ast.stmt], local: Dict[str, Any], mod: SourceModule) -> None:
        for st in stmts:
            self._tick()
            if isinstance(st, ast.Expr) and isinstance(st.value, ast.Constant):
                continue  # docstring
            if isinstance(st, (ast.Pass, ast.Global, ast.Nonlocal)):
                continue
            if isinstance(st, ast.Return):
                raise _Return(self.expr(st.value, local, mod) if st.value is not None else None)
            if isinstance(st, ast.Assign):
                value = self.expr(st.value, local, mod)
                for target in st.targets:
                    if not isinstance(target, ast.Name):
                        raise Undetermined("complex assignment")
                    local[target.id] = value
                continue
            if isinstance(st, ast.AnnAssign) and isinstance(st.target, ast.Name):
                if st.value is not None:
                    local[st.target.id] = self.expr(st.value, local, mod)
                continue
            if isinstance(st, ast.If):
                taken = st.body if self.expr(st.test, local, mod) else st.orelse
                self.block(taken, local, mod)
                continue
            if isinstance(st, ast.Try) and not st.finalbody:
                try:
                    self.block(st.body, local, mod)
                except (_Return, Undetermined):
                    raise
                except Exception as exc:  # noqa: BLE001 - mirrors the source's except
                    handler = self._handler_for(st.handlers, exc)
                    if handler is None:
                        raise
                    if handler.name:
                        raise Undetermined("bound exception")
                    self.block(handler.body, local, mod)
                else:
                    self.block(st.orelse, local, mod)
                continue
            raise Undetermined(type(st).__name__)

    @staticmethod
    def _handler_for(handlers: Sequence[ast.ExceptHandler], exc: Exception) -> Optional[ast.ExceptHandler]:
        for handler in handlers:
            if handler.type is None:
                return handler
            names = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
            for name in names:
                if not isinstance(name, ast.Name):
                    raise Undetermined("exception type")
                cls = getattr(builtins, name.id, None)
                if not (isinstance(cls, type) and issubclass(cls, BaseException)):
                    raise Undetermined("exception type " + name.id)
                if isinstance(exc, cls):
                    return handler
        return None

    def _tick(self) -> None:
        self.steps += 1
        if self.steps > self.MAX_STEPS:
            raise Undetermined("step budget")

    # -- expressions ----------------------------------------------------------
    def expr(self, node: ast.AST, local: Dict[str, Any], mod: SourceModule) -> Any:
        self._tick()
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            if node.id in local:
                return local[node.id]
            if node.id in mod.consts:
                return mod.consts[node.id]
            if node.id in mod.lazy:
                key = (mod.rel, node.id)
                if key in self._busy:
                    raise Undetermined("recursive constant " + node.id)
                self._busy.add(key)
                try:
                    return self.expr(mod.lazy[node.id], {}, mod)
                finally:
                    self._busy.discard(key)
            raise Undetermined("name " + node.id)
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            items = [self.expr(e, local, mod) for e in node.elts]
            return {ast.Tuple: tuple, ast.List: list, ast.Set: set}[type(node)](items)
        if isinstance(node, ast.Dict):
            if any(k is None for k in node.keys):
                raise Undetermined("dict unpacking")
            return {self.expr(k, local, mod): self.expr(v, local, mod)
                    for k, v in zip(node.keys, node.values)}
        if isinstance(node, ast.BoolOp):
            return self._boolop(node, local, mod)
        if isinstance(node, ast.UnaryOp):
            operand = self.expr(node.operand, local, mod)
            if isinstance(node.op, ast.Not):
                return not operand
            if isinstance(node.op, ast.USub):
                return -operand
            if isinstance(node.op, ast.UAdd):
                return +operand
            raise Undetermined("unary op")
        if isinstance(node, ast.BinOp):
            op = _BINOPS.get(type(node.op))
            if op is None:
                raise Undetermined("binary op")
            return op(self.expr(node.left, local, mod), self.expr(node.right, local, mod))
        if isinstance(node, ast.Compare):
            left = self.expr(node.left, local, mod)
            for op_node, comp in zip(node.ops, node.comparators):
                op = _CMPOPS.get(type(op_node))
                if op is None:
                    raise Undetermined("compare op")
                if mod.is_environ(comp) and isinstance(op_node, (ast.In, ast.NotIn)):
                    self.reads.append([str(left), "None"])
                right = self.expr(comp, local, mod)
                if not op(left, right):
                    return False
                left = right
            return True
        if isinstance(node, ast.IfExp):
            return self.expr(node.body if self.expr(node.test, local, mod) else node.orelse, local, mod)
        if isinstance(node, ast.Subscript):
            container = self.expr(node.value, local, mod)
            key = self.expr(node.slice, local, mod)
            if mod.is_environ(node.value):
                self.reads.append([str(key), "KeyError (required)"])
            return container[key]
        if isinstance(node, ast.Attribute):
            if mod.is_environ(node):
                return {}
            raise Undetermined("attribute")
        if isinstance(node, (ast.GeneratorExp, ast.ListComp, ast.SetComp)):
            return self._comprehension(node, local, mod)
        if isinstance(node, ast.Call):
            return self._call(node, local, mod)
        raise Undetermined(type(node).__name__)

    def _boolop(self, node: ast.BoolOp, local: Dict[str, Any], mod: SourceModule) -> Any:
        values = node.values
        result: Any = None
        for i, value_node in enumerate(values):
            mark = len(self.reads)
            result = self.expr(value_node, local, mod)
            if (isinstance(node.op, ast.Or) and i == 0 and len(values) > 1
                    and isinstance(value_node, ast.Call) and mod.is_direct_reader(value_node.func)
                    and len(self.reads) == mark + 1 and self.reads[mark][1] == "None"):
                # "os.getenv(K) or D": D is the code default of K.
                try:
                    self.reads[mark][1] = show(self.expr(values[1], {}, mod))
                except Exception:  # noqa: BLE001
                    pass
            if isinstance(node.op, ast.Or) and result:
                return result
            if isinstance(node.op, ast.And) and not result:
                return result
        return result

    def _comprehension(self, node: ast.AST, local: Dict[str, Any], mod: SourceModule) -> Any:
        if len(node.generators) != 1:
            raise Undetermined("nested comprehension")
        gen = node.generators[0]
        if not isinstance(gen.target, ast.Name) or gen.is_async:
            raise Undetermined("comprehension target")
        out = []
        for item in self.expr(gen.iter, local, mod):
            inner = dict(local)
            inner[gen.target.id] = item
            if all(self.expr(cond, inner, mod) for cond in gen.ifs):
                out.append(self.expr(node.elt, inner, mod))
        return set(out) if isinstance(node, ast.SetComp) else out

    def _call(self, node: ast.Call, local: Dict[str, Any], mod: SourceModule) -> Any:
        if any(isinstance(a, ast.Starred) for a in node.args):
            if len(node.args) != 1:
                raise Undetermined("starred call")
            args = list(self.expr(node.args[0].value, local, mod))
        else:
            args = [self.expr(a, local, mod) for a in node.args]
        kwargs = {}
        for kw in node.keywords:
            if kw.arg is None:
                raise Undetermined("keyword unpacking")
            kwargs[kw.arg] = self.expr(kw.value, local, mod)
        func = node.func
        if mod.is_direct_reader(func):
            if not args or not isinstance(args[0], str):
                raise Undetermined("dynamic ENV name")
            if len(args) > 1:
                default, raw = args[1], show(args[1])
            elif "default" in kwargs:
                default, raw = kwargs["default"], show(kwargs["default"])
            else:
                default, raw = None, "None"
            self.reads.append([args[0], raw])
            return default
        if isinstance(func, ast.Name):
            name = func.id
            if name in local:
                raise Undetermined("local callable")
            if name in mod.funcs:
                return self.call_function(mod, mod.funcs[name], args, kwargs)
            if name in mod.imports:
                target = self.repo.module_for_import(mod.imports[name][0])
                fn_name = mod.imports[name][1]
                if target is None or fn_name not in target.funcs:
                    raise Undetermined("import " + name)
                return self.call_function(target, target.funcs[fn_name], args, kwargs)
            if name in _SAFE_BUILTINS and not kwargs:
                return _SAFE_BUILTINS[name](*args)
            raise Undetermined("call " + name)
        if isinstance(func, ast.Attribute):
            obj = self.expr(func.value, local, mod)
            if isinstance(obj, str) and func.attr in _STR_METHODS:
                return getattr(obj, func.attr)(*args, **kwargs)
            if isinstance(obj, dict) and func.attr == "get" and not kwargs and 1 <= len(args) <= 2:
                return obj.get(*args)
        raise Undetermined("call")


def evaluate_function(repo: Repo, mod: SourceModule, fn_name: str) -> Tuple[bool, Any, List[List[str]]]:
    """(ok, value, reads) for a zero-argument call of a module function."""
    ev = Evaluator(repo)
    fn = mod.funcs.get(fn_name)
    if fn is None:
        return False, None, []
    try:
        value = ev.call_function(mod, fn)
    except Exception:  # noqa: BLE001 - Undetermined or a real error: unknown
        return False, None, ev.reads
    return True, value, ev.reads


def evaluate_expr(repo: Repo, mod: SourceModule, node: ast.AST) -> Tuple[bool, Any, List[List[str]]]:
    ev = Evaluator(repo)
    try:
        value = ev.expr(node, {}, mod)
    except Exception:  # noqa: BLE001
        return False, None, ev.reads
    return True, value, ev.reads


# --------------------------------------------------------------------------
# Extraction
# --------------------------------------------------------------------------
def module_tuple(mod: SourceModule, name: str) -> Tuple[str, ...]:
    value = mod.consts.get(name)
    if not isinstance(value, (tuple, list)) or not all(isinstance(v, str) for v in value):
        raise GeneratorError(f"{mod.rel}: {name} is not a static tuple of strings")
    return tuple(value)


def takes_no_required_args(fn: ast.FunctionDef) -> bool:
    spec = fn.args
    required = len(spec.posonlyargs + spec.args) - len(spec.defaults)
    kw_required = sum(1 for d in spec.kw_defaults if d is None)
    return required == 0 and kw_required == 0


def gate_rows(repo: Repo, mod: SourceModule, names: Sequence[str]) -> List[Dict[str, Any]]:
    """Section B.1 / B.2 rows: one per gate ENV, from its read sites."""
    rows = []
    for name in names:
        sites = mod.read_sites(name)
        readers: List[str] = []
        raws: List[str] = []
        effects: List[str] = []
        for site in sites:
            fn = mod.enclosing_function(site["node"])
            label = fn.name if fn is not None else "<module>"
            if label not in readers:
                readers.append(label)
            if site["default"] not in raws:
                raws.append(site["default"])
            if fn is None or mod.funcs.get(fn.name) is not fn or not takes_no_required_args(fn):
                continue
            ok, value, _reads = evaluate_function(repo, mod, fn.name)
            if ok and isinstance(value, _SCALARS):
                effect = f"{fn.name}() -> {show(value)}"
                if effect not in effects:
                    effects.append(effect)
        rows.append({"name": name, "file": mod.rel, "readers": readers,
                     "raw": raws, "effects": effects})
    return rows


def surface_gate_dict(mod: SourceModule) -> List[Tuple[str, ast.AST]]:
    fn = mod.funcs.get("surface_gate_states")
    if fn is None:
        raise GeneratorError(f"{mod.rel}: surface_gate_states() not found")
    for node in ast.walk(fn):
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Dict):
            out = []
            for key, value in zip(node.value.keys, node.value.values):
                if isinstance(key, ast.Constant) and isinstance(key.value, str):
                    out.append((key.value, value))
            return out
    raise GeneratorError(f"{mod.rel}: surface_gate_states() returns no dict literal")


def static_env_names(repo: Repo, mod: SourceModule, node: ast.AST, depth: int = 0,
                     seen: Optional[set] = None) -> List[str]:
    """ENV names a gate expression can read: constant name arguments, plus
    names read by the module functions it calls (two levels deep)."""
    seen = set() if seen is None else seen
    names: List[str] = []

    def add(name: Optional[str]) -> None:
        if name and ENV_NAME_RE.match(name) and name not in names:
            names.append(name)

    for call in [n for n in ast.walk(node) if isinstance(n, ast.Call)]:
        if mod.is_direct_reader(call.func) and call.args:
            add(mod.const_str(call.args[0]))
            continue
        target_mod, target_fn = None, None
        if isinstance(call.func, ast.Name):
            fname = call.func.id
            if fname in mod.funcs:
                target_mod, target_fn = mod, mod.funcs[fname]
            elif fname in mod.imports:
                other = repo.module_for_import(mod.imports[fname][0])
                if other is not None and mod.imports[fname][1] in other.funcs:
                    target_mod, target_fn = other, other.funcs[mod.imports[fname][1]]
        if target_fn is None:
            continue
        if (target_fn.name in target_mod.env_funcs or target_fn.name in target_mod.helper_readers):
            for arg in call.args:
                add(mod.const_str(arg))
        key = (target_mod.rel, target_fn.name)
        if depth < 2 and key not in seen:
            seen.add(key)
            for name in static_env_names(repo, target_mod, target_fn, depth + 1, seen):
                add(name)
    return names


def engine_rows(repo: Repo, mod: SourceModule) -> List[Dict[str, Any]]:
    rows = []
    for key, value_node in surface_gate_dict(mod):
        names = static_env_names(repo, mod, value_node)
        ok, value, reads = evaluate_expr(repo, mod, value_node)
        raw_by_name: Dict[str, str] = {}
        for rname, rdefault in reads:
            raw_by_name.setdefault(rname, rdefault)
        for rname, _ in reads:
            if ENV_NAME_RE.match(rname) and rname not in names:
                names.append(rname)
        raws = []
        for name in names:
            raw = raw_by_name.get(name)
            if raw is None:
                site_defaults = [s["default"] for s in mod.read_sites(name)]
                raw = " / ".join(dict.fromkeys(site_defaults)) if site_defaults else "-"
            raws.append(raw)
        if not names:
            unset_value = "-"
        elif ok and isinstance(value, _SCALARS):
            unset_value = show(value)
        else:
            unset_value = "-"
        rows.append({"key": key, "names": names, "raw": raws, "unset": unset_value})
    return rows


def env_first_call(mod: Optional[SourceModule], func_name: str, member: str) -> Optional[Dict[str, Any]]:
    """The first call func_name(...) whose constant arguments include member."""
    if mod is None:
        return None
    for node in ast.walk(mod.tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == func_name):
            continue
        names = [mod.const_str(a) for a in node.args]
        if member not in names:
            continue
        default: Optional[str] = None
        for kw in node.keywords:
            if kw.arg == "default":
                ok, value, _ = evaluate_expr(mod.repo, mod, kw.value)
                default = show(value) if ok else "computed"
        parent = mod.parents.get(node)
        if (default is None and isinstance(parent, ast.BoolOp) and isinstance(parent.op, ast.Or)
                and parent.values[0] is node and len(parent.values) > 1):
            ok, value, _ = evaluate_expr(mod.repo, mod, parent.values[1])
            default = show(value) if ok else "computed"
        return {"names": [n for n in names if n], "default": default}
    return None


def coerce_version_tokens(mod: SourceModule) -> List[str]:
    fn = mod.funcs.get("_coerce_version")
    if fn is None:
        return []
    for node in ast.walk(fn):
        if isinstance(node, ast.Compare) and isinstance(node.ops[0], ast.In):
            try:
                values = ast.literal_eval(node.comparators[0])
            except (ValueError, TypeError, SyntaxError):
                continue
            if isinstance(values, (set, frozenset, tuple, list)):
                return sorted(str(v) for v in values)
    return []


def read_by(repo: Repo, name: str) -> Tuple[List[str], List[str]]:
    """(files in PLATFORM_READ_FILES that read name, distinct raw defaults)."""
    files: List[str] = []
    defaults: List[str] = []
    shell_re = re.compile(r"\$\{?" + re.escape(name) + r"\b")
    for rel in PLATFORM_READ_FILES:
        if rel.endswith(".sh"):
            text = repo.text(rel)
            if text and shell_re.search(text):
                files.append(rel)
            continue
        mod = repo.module(rel)
        if mod is None:
            continue
        sites = mod.read_sites(name)
        if sites:
            files.append(rel)
            for site in sites:
                if site["default"] not in defaults:
                    defaults.append(site["default"])
    return files, defaults


def verifier_flags(mod: SourceModule) -> Dict[str, Any]:
    flags_value = None
    for node in mod.tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else (
            [node.target] if isinstance(node, ast.AnnAssign) else [])
        for target in targets:
            if isinstance(target, ast.Name) and target.id == "FLAGS" and node.value is not None:
                try:
                    flags_value = ast.literal_eval(node.value)
                except (ValueError, TypeError, SyntaxError):
                    flags_value = None
    if not isinstance(flags_value, list):
        raise GeneratorError(f"{mod.rel}: FLAGS is not a static list")

    def names_of(const: str) -> set:
        # A set, tuple or dict literal; a dict contributes its keys.
        return set(mod.consts.get(const) or ())

    return {"flags": flags_value,
            "workflow": names_of("_WORKFLOW_SCOPED"),
            "value": names_of("_VALUE_FLAGS"),
            "disarms": names_of("DECLARED_DISARMS")}


# --------------------------------------------------------------------------
# Owner-filled expected values
# --------------------------------------------------------------------------
class Expected:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.present = path.is_file()
        self.values: Dict[str, str] = {}
        self.recorded = ""
        self.warnings: List[str] = []
        if not self.present:
            return
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise GeneratorError(f"{path}: not readable JSON ({type(exc).__name__})") from None
        if not isinstance(data, dict) or not isinstance(data.get("expected"), dict):
            raise GeneratorError(f"{path}: expected a JSON object with an 'expected' object")
        recorded = data.get("recorded")
        if isinstance(recorded, str) and re.match(r"^\d{4}-\d{2}-\d{2}$", recorded):
            self.recorded = recorded
        for name, value in sorted(data["expected"].items()):
            name = str(name)
            if not ENV_NAME_RE.match(name):
                self.warnings.append(f"ignored a key that is not an ENV name ({len(name)} chars)")
                continue
            if is_withheld_name(name):
                self.warnings.append(f"withheld a name under owner decision D7 ({len(name)} chars)")
                continue
            self.values[name] = value if isinstance(value, str) else json.dumps(value)

    def has(self, name: str) -> bool:
        return name in self.values

    def render(self, name: str) -> str:
        if name not in self.values:
            return NOT_RECORDED
        value = self.values[name]
        if value == "":
            return "empty string"
        if value.strip().lower() == "unset":
            return "unset"
        if (not PLAIN_VALUE_RE.match(value) or SECRETISH_VALUE_RE.match(value)
                or "@" in value or "://" in value):
            return "(withheld: not a plain flag value)"
        return code(value)


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------
def _table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> List[str]:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join(" --- " for _ in header) + "|"]
    for row in rows:
        out.append("| " + " | ".join(row) + " |")
    return out


def _files(files: Sequence[str]) -> str:
    return ", ".join(files) if files else "no static read found in the files checked"


def build_contract(root: Path, expected_path: Optional[Path] = None) -> Dict[str, Any]:
    """Everything the document says, as data. Raises GeneratorError."""
    repo = Repo(Path(root))
    main_mod = repo.require(MAIN_FILE)
    pf_mod = repo.require(PF_FILE)
    ob_mod = repo.require(OB_FILE)
    engine_mod = repo.require(ENGINE_FILE)
    verifier = repo.require(VERIFIER_FILE)
    expected = Expected(Path(expected_path) if expected_path else repo.root / DEFAULT_EXPECTED)
    return {
        "repo": repo,
        "main": main_mod,
        "pf_names": module_tuple(main_mod, "_PF_GATE_ENVS"),
        "ob_names": module_tuple(main_mod, "_OB_GATE_ENVS"),
        "deploy_names": module_tuple(main_mod, "_DEPLOY_ENVS"),
        "pf_rows": gate_rows(repo, pf_mod, module_tuple(main_mod, "_PF_GATE_ENVS")),
        "ob_rows": gate_rows(repo, ob_mod, module_tuple(main_mod, "_OB_GATE_ENVS")),
        "engine_rows": engine_rows(repo, engine_mod),
        "verifier": verifier_flags(verifier),
        "expected": expected,
    }


def _platform_rows(data: Dict[str, Any]) -> List[Tuple[str, str, str, str]]:
    """(key, contract, read by / note, code default) for section A."""
    repo: Repo = data["repo"]
    main_mod: SourceModule = data["main"]
    config_mod = repo.module("config.py")
    env_mod = repo.module("env.py")
    tokens = coerce_version_tokens(main_mod)
    token_text = ", ".join(repr(t) for t in tokens) if tokens else "(not found)"
    version_call = env_first_call(config_mod, "_env_first", "APP_VERSION")
    env_call = env_first_call(config_mod, "_env_first", "APP_ENV")
    legacy_call = env_first_call(env_mod, "get_first_env", "APP_VERSION")

    def order(call: Optional[Dict[str, Any]], rel: str) -> str:
        if not call:
            return f"{rel}: read order not found statically (re-check)"
        default = call["default"] if call["default"] is not None else "None"
        return (f"{rel} reads " + ", then ".join(call["names"])
                + f" (first set wins; default {default})")

    rows: List[Tuple[str, str, str, str]] = []
    version_note = (order(version_call, "config.py") + "; main.py turns "
                    + token_text + " into APP_ENTRY_VERSION")
    for key in ("TFB_APP_VERSION", "APP_VERSION"):
        files, _ = read_by(repo, key)
        rows.append((key, MUST_UNSET,
                     version_note + ". Read by: " + _files(files),
                     "unset -> /health app_version == entry_version"))
    for key in ("SERVICE_VERSION", "VERSION"):
        files, _ = read_by(repo, key)
        rows.append((key, "SHOULD be unset",
                     order(legacy_call, "env.py")
                     + "; used only when config.py cannot load. Read by: " + _files(files),
                     "unset"))
    env_names = env_call["names"] if env_call else ["APP_ENV"]
    env_default = (env_call or {}).get("default") or "'production'"
    for key in env_names:
        files, _ = read_by(repo, key)
        if key == "APP_ENV":
            contract = "unset, or production"
        else:
            contract = "SHOULD be unset (read before APP_ENV, so a stale value shadows it)"
        rows.append((key, contract,
                     order(env_call, "config.py") + ". Read by: " + _files(files),
                     env_default))
    for key, contract, note in (
            ("PORT", "supplied by Render; do not set by hand",
             "scripts/start_web.sh binds it (see its header)"),
            ("WEB_CONCURRENCY", f"owner setting (Procfile names env group {ENV_GROUP})",
             "worker count for scripts/start_web.sh (see its header)"),
            ("REDIS_URL", "secret connection string: name only, never a value in this repo",
             "engine L2 caches (engine_gates *_redis rows) and scripts/worker.py")):
        files, defaults = read_by(repo, key)
        default_text = " / ".join(defaults) if defaults else "-"
        if "scripts/start_web.sh" in files:
            default_text += " (Python); the shell launcher sets its own"
        rows.append((key, contract, note + ". Read by: " + _files(files), default_text))
    for key in data["deploy_names"]:
        rows.append((key, "supplied by Render at runtime; never set by hand",
                     "main.py _DEPLOY_ENVS; /health deploy." + key.lower(),
                     "'unset' on /health when absent"))
    return rows


def render_markdown(data: Dict[str, Any]) -> Tuple[str, int]:
    """(document text, number of names withheld under D7)."""
    expected: Expected = data["expected"]
    withheld: set = set()

    def keep(name: str) -> bool:
        if is_withheld_name(name):
            withheld.add(name)
            return False
        return True

    def exp(name: str) -> str:
        return expected.render(name)

    lines: List[str] = []
    w = lines.append
    w("# Render ENV contract")
    w("")
    w(f"Generated by `{REGENERATE_COMMAND}` (render_env_contract {SCRIPT_VERSION}). "
      f"Do not edit by hand: regenerate and commit. `{REGENERATE_COMMAND} --check` "
      "exits 1 when this file is stale.")
    w("")
    w("Build Spec item H-8. This is a static contract read from the source code by AST. "
      "It is not a readback of the live Render service. It lists the ENV names the web "
      "service reads and their code defaults, never a secret value.")
    w("")
    w("Rules:")
    w("")
    w("- Names and code defaults only. A default shown as `-` could not be evaluated "
      "statically and is not guessed.")
    w("- Owner decision D7 (public repository) is open, so names that carry or switch "
      "authentication, tokens or other secrets are left out of this document. "
      "WITHHELD_COUNT_PLACEHOLDER")
    w(f"- Render takes ENV from two places: the service's own Environment list and every "
      f"linked Environment Group (Procfile names the group `{ENV_GROUP}`). When this "
      "contract says a key must be unset, check both.")
    if expected.present:
        when = f" (recorded {expected.recorded})" if expected.recorded else ""
        w(f"- Expected production values come from `{DEFAULT_EXPECTED}`{when}; a row "
          "the owner has not filled says `not recorded`.")
    else:
        w(f"- expected production value: not recorded. `{DEFAULT_EXPECTED}` is absent; "
          "every row says `not recorded` until the owner fills that file.")
    w("")

    # ---- A ---------------------------------------------------------------
    w("## A. Platform keys outside the TFB_ prefix")
    w("")
    w("### Why TFB_APP_VERSION and APP_VERSION MUST be unset")
    w("")
    w("`app_version` on /health is not a code constant. config.py reads TFB_APP_VERSION, "
      "then APP_VERSION, and the first one set wins. Only when both are unset does main.py "
      "fall back to its APP_ENTRY_VERSION, so /health then reports app_version equal to "
      "entry_version. On 13 Oct 2026 the owner's /health showed app_version 5.111.0 while "
      "entry_version was 8.14.3. The literal 5.111.0 is not in main.py, config.py, env.py "
      "or core/config.py, so one of the two keys is still set in the service ENV or in a "
      f"linked env group (`{ENV_GROUP}`). Delete it in both places. Read-back: GET /health "
      "shows app_version equal to entry_version.")
    w("")
    a_rows = []
    for key, contract, note, default in _platform_rows(data):
        if not keep(key):
            continue
        if contract == MUST_UNSET:
            expected_cell = MUST_UNSET
            if expected.has(key) and expected.values[key].strip().lower() != "unset":
                expected.warnings.append(f"ignored a value for {key}: the contract says {MUST_UNSET}")
        else:
            expected_cell = exp(key)
        a_rows.append([code(key), cell(contract), cell(note), cell(default), expected_cell])
    lines.extend(_table(["Key", "Contract", "Read by (static)", "Code default",
                         "Expected production value"], a_rows))
    w("")

    # ---- B ---------------------------------------------------------------
    w("## B. Gates visible on /health")
    w("")
    w("main.py reports these on every status payload. `pf_gates` shows the raw ENV value "
      "of each key below, or the word `unset`. `engine_gates` shows the state the engine "
      "computes from its ENV.")
    w("")
    w("`Raw code default` is the default written at the read. `Effective when unset` is "
      "what the reading function returns when every ENV is unset, by static evaluation. "
      "`-` means the reader takes arguments or does more than the evaluator follows; the "
      "raw default then applies at the read.")
    w("")
    for title, rows in (
            ("B.1 pf_gates.portfolio_actions (main.py `_PF_GATE_ENVS`, read in "
             + PF_FILE + ")", data["pf_rows"]),
            ("B.2 pf_gates.opportunity_builder (main.py `_OB_GATE_ENVS`, read in "
             + OB_FILE + ")", data["ob_rows"])):
        w("### " + title)
        w("")
        b_rows = []
        for row in rows:
            if not keep(row["name"]):
                continue
            readers = ", ".join(code(r) for r in row["readers"]) or "no read found (re-check)"
            raw = " / ".join(row["raw"]) if row["raw"] else "-"
            effects = "; ".join(row["effects"]) if row["effects"] else "-"
            b_rows.append([code(row["name"]), readers, cell(raw), cell(effects), exp(row["name"])])
        lines.extend(_table(["Key", "Read in", "Raw code default", "Effective when unset",
                             "Expected production value"], b_rows))
        w("")
    w("### B.3 engine_gates (" + ENGINE_FILE + " `surface_gate_states()`)")
    w("")
    w("ENV names are derived statically from each entry's expression and the functions it "
      "calls. Entries with no ENV read report runtime state, not configuration.")
    w("")
    e_rows = []
    for row in data["engine_rows"]:
        shown = [(n, r) for n, r in zip(row["names"], row["raw"]) if keep(n)]
        if row["names"] and not shown:
            continue
        if shown:
            names_cell = ", ".join(code(n) for n, _ in shown)
            raw_cell = cell("; ".join(f"{n}={r}" if len(shown) > 1 else r for n, r in shown))
            if len(shown) == 1 or not any(expected.has(n) for n, _ in shown):
                exp_cell = exp(shown[0][0]) if len(shown) == 1 else NOT_RECORDED
            else:
                exp_cell = "; ".join(f"{n}: {exp(n)}" for n, _ in shown)
        else:
            names_cell, raw_cell, exp_cell = "runtime state (no ENV read)", "-", "-"
        e_rows.append([code(row["key"]), names_cell, raw_cell, cell(row["unset"]), exp_cell])
    lines.extend(_table(["/health key", "ENV read", "Raw code default",
                         "/health value when unset", "Expected production value"], e_rows))
    w("")

    # ---- C ---------------------------------------------------------------
    ver = data["verifier"]
    on_health = set(data["pf_names"]) | set(data["ob_names"])
    for row in data["engine_rows"]:
        on_health.update(row["names"])
    w("## C. Render-scoped entries of scripts/verify_deployment.py FLAGS")
    w("")
    w("Entries in the verifier's `_WORKFLOW_SCOPED` set live in GitHub workflow env and "
      "are left out here. `Manifest default` is the verifier's declared default.")
    w("")
    c_rows = []
    for entry in ver["flags"]:
        name, default, meaning, kill = entry[0], entry[1], entry[2], bool(entry[3])
        if name in ver["workflow"] or not keep(name):
            continue
        kind = "value" if name in ver["value"] else "toggle"
        kill_cell = "yes" if kill else "no"
        if name in ver["disarms"]:
            kill_cell += " (declared disarm)"
        c_rows.append([code(name), code(default) if default != "" else code("''"), cell(meaning),
                       kill_cell, kind, "yes" if name in on_health else "no", exp(name)])
    lines.extend(_table(["Key", "Manifest default", "Meaning when armed", "Kill switch",
                         "Kind", "Also on /health", "Expected production value"], c_rows))
    w("")

    # ---- D ---------------------------------------------------------------
    w("## D. Every other flag: docs/flags.md")
    w("")
    w(f"`{FLAGS_DOC}` is the full static inventory of TFB_ and TRACK_ flags, generated by "
      f"`python scripts/flag_registry.py --output {FLAGS_DOC}`. Its rows whose consumer "
      "lane includes `Render/library` are the remaining Render-side names. They are not "
      "copied here, and that file is not a readback of Render either.")
    w("")

    # ---- refresh ---------------------------------------------------------
    w("## Refreshing this contract")
    w("")
    w(f"- After changing any input (main.py gate tables, the decision modules' ENV "
      f"readers, surface_gate_states, verify_deployment FLAGS or `{DEFAULT_EXPECTED}`), "
      f"run `{REGENERATE_COMMAND}` and commit the result.")
    w(f"- `{DEFAULT_EXPECTED}` is owner-filled: "
      '`{"recorded": "YYYY-MM-DD", "expected": {"NAME": "value"}}`. Plain values only; '
      "the generator withholds anything that does not look like a plain flag value, and "
      "names covered by D7 are never printed. The word `unset` records a key that must "
      "stay unset.")
    w("")
    text = "\n".join(lines)
    note = (f"{len(withheld)} such name(s) from sections A-C were withheld."
            if withheld else "No name in sections A-C was withheld by this rule.")
    text = text.replace("WITHHELD_COUNT_PLACEHOLDER", note)
    return ascii_text(text), len(withheld)


def generate(root: Path, expected_path: Optional[Path] = None) -> str:
    data = build_contract(root, expected_path)
    text, _ = render_markdown(data)
    return text


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--output", default=None,
                        help=f"output path ('-' for stdout; default {DEFAULT_OUTPUT} under --root)")
    parser.add_argument("--root", default=None, help="repository root to scan")
    parser.add_argument("--expected", default=None,
                        help=f"expected-values JSON (default {DEFAULT_EXPECTED} under --root)")
    parser.add_argument("--check", action="store_true",
                        help="exit 1 when the output file differs from the generator output")
    args = parser.parse_args(argv)
    tag = f"[render_env_contract v{SCRIPT_VERSION}]"
    root = Path(args.root) if args.root else _repo_root()
    output = args.output if args.output is not None else str(root / DEFAULT_OUTPUT)
    try:
        data = build_contract(root, Path(args.expected) if args.expected else None)
        text, withheld = render_markdown(data)
    except GeneratorError as exc:
        print(f"{tag} ERROR: {exc}", file=sys.stderr)
        return 2
    for warning in data["expected"].warnings:
        print(f"{tag} WARN: {data['expected'].path.name}: {warning}", file=sys.stderr)
    if output == "-":
        if args.check:
            print(f"{tag} ERROR: --check needs a file, not stdout", file=sys.stderr)
            return 2
        sys.stdout.write(text)
        return 0
    path = Path(output)
    current = path.read_text(encoding="utf-8") if path.is_file() else None
    if args.check:
        if current == text:
            print(f"{tag} OK: {output} is current")
            return 0
        state = "is missing" if current is None else "differs from the generator output"
        print(f"{tag} DRIFT: {output} {state}. Regenerate with: {REGENERATE_COMMAND}"
              " and commit the result.")
        return 1
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    sizes = (f"A+B+C rows: pf={len(data['pf_rows'])} ob={len(data['ob_rows'])} "
             f"engine={len(data['engine_rows'])} withheld={withheld}")
    print(f"{tag} wrote {output} ({sizes})")
    if current is not None and current != text:
        print(f"{tag} WARN: {output} changed; commit the regenerated file.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
