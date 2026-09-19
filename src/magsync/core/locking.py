"""Portable advisory file locks.

POSIX uses ``flock``; Windows falls back to ``msvcrt.locking`` on the first
byte. Importing this module never fails, so the base CLI stays importable on
platforms without ``fcntl``; only an actual lock attempt reports the absence
of any locking primitive.
"""

from __future__ import annotations

import errno
import os

try:
    import fcntl
except ImportError:  # pragma: no cover - exercised on Windows only
    fcntl = None

try:
    import msvcrt
except ImportError:
    msvcrt = None

# ``O_NOFOLLOW`` is POSIX-only; elsewhere the flag is simply not applied.
NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)


def lock(fd: int, *, blocking: bool) -> None:
    """Take an exclusive advisory lock; raise ``OSError`` when unavailable."""
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        return
    if msvcrt is not None:  # pragma: no cover - Windows
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_LOCK if blocking else msvcrt.LK_NBLCK, 1)
        return
    raise OSError(errno.ENOSYS, "advisory file locking is unavailable")


def unlock(fd: int) -> None:
    """Release a lock taken by :func:`lock`."""
    if fcntl is not None:
        fcntl.flock(fd, fcntl.LOCK_UN)
        return
    if msvcrt is not None:  # pragma: no cover - Windows
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
