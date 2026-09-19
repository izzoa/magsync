"""Container liveness probe, independent of discovery interval or source health."""
from __future__ import annotations

import os
import time
from pathlib import Path


# Touched by every live runtime; checked by the Docker HEALTHCHECK.
HEALTH_CHECK_PATH = Path('/tmp/magsync-healthy')


def healthy(path: Path | None = None, *, now: float | None = None, threshold: float | None = None) -> bool:
    path = HEALTH_CHECK_PATH if path is None else path
    if threshold is None:
        threshold = float(os.getenv('MAGSYNC_SERVICE__HEARTBEAT_STALE_SECONDS', '30'))
    try:
        age = (time.time() if now is None else now) - path.stat().st_mtime
        return 0 <= age < threshold
    except OSError:
        return False


if __name__ == '__main__':
    raise SystemExit(0 if healthy() else 1)
