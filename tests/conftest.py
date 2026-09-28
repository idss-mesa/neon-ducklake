"""Hermetic identity for the whole suite: a fixed CyVerse user and default paths, whoever runs the tests.

``neon_ducklake.env`` resolves the user and the iRODS/precedent paths at import time, so this runs before any
test module imports it. Tests that need other values monkeypatch the ``env`` attributes.
"""

from __future__ import annotations

import os

TEST_USER = "testuser"

os.environ["CYVERSE_USERNAME"] = TEST_USER
for _var in ("MESA_MCP_IRODS__USER", "NEON_DUCKLAKE_ROOT", "NEON_DUCKLAKE_IRODS_ROOT", "NEON_DUCKLAKE_PRECEDENT_LAKE",
             "NEON_DUCKLAKE_PRECEDENT_DATA", "NEON_DUCKLAKE_PRECEDENT_VERSION", "NEON_DUCKLAKE_PRECEDENT_IRODS"):
    os.environ.pop(_var, None)
