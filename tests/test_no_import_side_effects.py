"""Every test module imports offline without changing shared policy or logging."""
import subprocess
import sys
from pathlib import Path


RUNNER = r'''
import importlib.util, json, logging, os, pathlib, socket, sys
root = pathlib.Path(sys.argv[1])
sys.path.insert(0, str(root))
sys.path.insert(0, str(root / 'tests'))
sys.path.insert(0, str(root / 'scripts'))
# Initialize the application's intentional startup logging once, before
# measuring test imports. The test modules must not reconfigure it.
import main
def no_network(*args, **kwargs):
    raise AssertionError('Network attempted while importing a test module')
socket.socket.connect = no_network
socket.create_connection = no_network
logger = logging.getLogger()
def logger_state():
    return (logger.level, logger.disabled, tuple(logger.handlers), tuple(logger.filters),
            tuple((h.level, h.formatter, tuple(h.filters)) for h in logger.handlers))
failures = []
for path in sorted((root / 'tests').glob('test_*.py')):
    env_before = dict(os.environ)
    log_before = logger_state()
    name = 'tests.' + path.stem
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    except BaseException as exc:
        # Optional dependencies are permitted to skip during collection.
        import pytest
        if not isinstance(exc, pytest.skip.Exception):
            failures.append(f'{path.name}: import raised {type(exc).__name__}: {exc}')
    if dict(os.environ) != env_before:
        changed = sorted(k for k in set(env_before) | set(os.environ)
                         if env_before.get(k) != os.environ.get(k))
        failures.append(f'{path.name}: changed environment keys {changed}')
    if logger_state() != log_before:
        failures.append(f'{path.name}: changed root logging')
print('@@IMPORT_AUDIT@@' + json.dumps(failures))
raise SystemExit(bool(failures))
'''


def test_every_module_preserves_environment_and_root_logger_on_import():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, "-c", RUNNER, str(root)], cwd=root,
                            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout[-16000:] + result.stderr[-8000:]
    assert "@@IMPORT_AUDIT@@[]" in result.stdout
