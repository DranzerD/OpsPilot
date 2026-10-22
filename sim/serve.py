"""Run one simulated service: `python -m sim.serve <service-name>`."""

from __future__ import annotations

import os
import sys

# Must be set before the FastAPI instrumentation is imported: emit the stable HTTP semantic
# conventions (http.server.request.duration in seconds) rather than the legacy ones.
os.environ.setdefault("OTEL_SEMCONV_STABILITY_OPT_IN", "http")

import uvicorn

from sim.app import create_app
from sim.config import HOST, load_topology


def main(argv: list[str]) -> int:
    services = load_topology().services
    if len(argv) != 1 or argv[0] not in services:
        print(f"usage: python -m sim.serve <{'|'.join(services)}>", file=sys.stderr)
        return 2
    name = argv[0]
    uvicorn.run(create_app(name), host=HOST, port=services[name].port, access_log=False, log_config=None)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
