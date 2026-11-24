"""Run a RustFS binary on loopback with a caller-owned disposable volume."""

from contextlib import closing, contextmanager
from http.client import HTTPConnection
import os
import socket
import subprocess
import time

if __package__:
    from .sts_root_regression import stop_process
else:
    from sts_root_regression import stop_process


HOST = "127.0.0.1"


@contextmanager
def local_rustfs(binary, credentials, root, env_overrides):
    with socket.socket() as listener:
        listener.bind((HOST, 0))
        port = listener.getsockname()[1]
    (root / "data").mkdir(exist_ok=True)
    env = {
        "PATH": os.environ.get("PATH", os.defpath),
        "RUSTFS_ADDRESS": f"{HOST}:{port}", "RUSTFS_VOLUMES": str(root / "data"),
        "RUSTFS_ACCESS_KEY": credentials.access_key, "RUSTFS_SECRET_KEY": credentials.secret_key,
        "RUSTFS_CONSOLE_ENABLE": "false", "RUSTFS_CONSOLE_ADDRESS": f"{HOST}:0",
        "RUSTFS_OBS_LOG_DIRECTORY": str(root / "logs"), "RUSTFS_OBS_LOGGER_LEVEL": "warn",
        **env_overrides,
    }
    with (root / "server.log").open("ab") as log:
        process = subprocess.Popen([str(binary.resolve())], env=env, cwd=root, start_new_session=True,
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=log)
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"RustFS exited during startup ({process.returncode})")
            try:
                with closing(HTTPConnection(HOST, port, timeout=1)) as conn:
                    conn.request("GET", "/health/ready")
                    if conn.getresponse().status == 200:
                        break
            except OSError:
                pass
            time.sleep(0.2)
        else:
            raise RuntimeError("RustFS readiness deadline expired")
        yield port
    finally:
        stop_process(process)
