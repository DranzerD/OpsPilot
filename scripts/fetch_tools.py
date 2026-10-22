"""Download the native Prometheus and OpenTelemetry Collector binaries into tools/.

No Docker: both ship as standalone Windows executables. Each archive is verified against the
SHA-256 checksum published with its GitHub release before it is extracted.

    python scripts/fetch_tools.py
"""

from __future__ import annotations

import hashlib
import shutil
import sys
import tarfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TOOLS = ROOT / "tools"
DOWNLOADS = TOOLS / "downloads"

PROMETHEUS_VERSION = "3.15.1"
OTELCOL_VERSION = "0.163.2"

_PROM_BASE = f"https://github.com/prometheus/prometheus/releases/download/v{PROMETHEUS_VERSION}"
_OTEL_BASE = (
    f"https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v{OTELCOL_VERSION}"
)

TOOLS_SPEC = {
    "prometheus": {
        "archive": f"prometheus-{PROMETHEUS_VERSION}.windows-amd64.zip",
        "url": f"{_PROM_BASE}/prometheus-{PROMETHEUS_VERSION}.windows-amd64.zip",
        "checksums": f"{_PROM_BASE}/sha256sums.txt",
        "exe": "prometheus.exe",
    },
    "otelcol": {
        "archive": f"otelcol-contrib_{OTELCOL_VERSION}_windows_amd64.tar.gz",
        "url": f"{_OTEL_BASE}/otelcol-contrib_{OTELCOL_VERSION}_windows_amd64.tar.gz",
        "checksums": f"{_OTEL_BASE}/otelcol-contrib_{OTELCOL_VERSION}_windows_amd64.tar.gz.sha256",
        "exe": "otelcol-contrib.exe",
    },
}


def _fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as resp:
        return resp.read().decode()


def _expected_sha256(checksums: str, archive: str) -> str:
    # Both formats are "<hex>  <filename>" lines; the otelcol file may hold only the hash.
    for line in checksums.splitlines():
        parts = line.split()
        if len(parts) == 1:
            return parts[0].lower()
        if len(parts) >= 2 and parts[-1].lstrip("*") == archive:
            return parts[0].lower()
    raise RuntimeError(f"no checksum for {archive}")


def _download(url: str, dest: Path) -> None:
    print(f"downloading {url}")
    with urllib.request.urlopen(url, timeout=600) as resp, open(dest, "wb") as out:
        shutil.copyfileobj(resp, out, length=1 << 20)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _extract_exe(archive: Path, exe: str, dest: Path) -> None:
    # Extract only the executable, never arbitrary archive paths.
    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive) as zf:
            member = next(n for n in zf.namelist() if Path(n).name == exe)
            with zf.open(member) as src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)
    else:
        with tarfile.open(archive) as tf:
            member = next(m for m in tf.getmembers() if Path(m.name).name == exe and m.isfile())
            src = tf.extractfile(member)
            assert src is not None
            with src, open(dest, "wb") as out:
                shutil.copyfileobj(src, out)


def fetch(name: str) -> Path:
    spec = TOOLS_SPEC[name]
    exe_path = TOOLS / spec["exe"]
    if exe_path.exists():
        print(f"{name}: already present at {exe_path}")
        return exe_path
    DOWNLOADS.mkdir(parents=True, exist_ok=True)
    archive = DOWNLOADS / spec["archive"]
    expected = _expected_sha256(_fetch_text(spec["checksums"]), spec["archive"])
    if not archive.exists() or _sha256(archive) != expected:
        _download(spec["url"], archive)
    actual = _sha256(archive)
    if actual != expected:
        archive.unlink()
        raise RuntimeError(f"{name}: checksum mismatch (expected {expected}, got {actual})")
    print(f"{name}: checksum ok")
    _extract_exe(archive, spec["exe"], exe_path)
    print(f"{name}: installed {exe_path}")
    return exe_path


def main() -> int:
    for name in TOOLS_SPEC:
        fetch(name)
    return 0


if __name__ == "__main__":
    sys.exit(main())
