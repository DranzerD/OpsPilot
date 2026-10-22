import os
import tempfile
from pathlib import Path

# Must be set before sim.config is imported: tests never touch var/platform.db.
_TMP = Path(tempfile.mkdtemp(prefix="opspilot-tests-"))
os.environ["PLATFORM_DB_PATH"] = str(_TMP / "platform.db")
os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] = "http"
