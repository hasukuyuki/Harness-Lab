"""Shared pytest path bootstrap for backend test suites.

This keeps both `backend.app...` and historical `app...` imports working when
the suite is launched via `pytest backend/tests -q` from the repo root.
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = REPO_ROOT / "backend"

for candidate in (str(REPO_ROOT), str(BACKEND_ROOT)):
    if candidate not in sys.path:
        sys.path.insert(0, candidate)
