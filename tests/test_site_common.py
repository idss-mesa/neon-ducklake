"""Shared fixtures/fakes for the hermetic site tests (no network, no iRODS)."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from neon_ducklake import env, state

SITE = "TEST"


@pytest.fixture
def site_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect every project path (and the journal) into tmp_path."""
    root = tmp_path / "root"
    root.mkdir()
    monkeypatch.setattr(env, "ROOT", root)
    monkeypatch.setattr(env, "API_DIR", root / "api")
    monkeypatch.setattr(env, "RUNS_DIR", root / "api" / "runs")
    monkeypatch.setattr(env, "LAKE_DIR", root / "ducklake")
    monkeypatch.setattr(env, "SITES_DIR", root / "sites")
    monkeypatch.setattr(env, "WORK_DIR", root / "work")
    monkeypatch.setattr(env, "LOCK_DIR", root / "work" / "locks")
    monkeypatch.setattr(env, "MESA_CACHE_DIR", root / "work" / "mesa-cache")
    monkeypatch.setattr(env, "QUARANTINE_DIR", root / "work" / "quarantine")
    monkeypatch.setattr(env, "IRODS_ROOT", "/zone/home/u/proj")
    monkeypatch.setattr(env, "DUCKDB_SETTINGS", {"memory_limit": "2GB", "threads": 2})
    monkeypatch.setattr(state, "DB_PATH", root / "work" / "state.sqlite")
    return root


def md5(data: bytes) -> str:
    return hashlib.md5(data, usedforsecurity=False).hexdigest()


def write_gz(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(gzip.compress(json.dumps(obj).encode()))


# ---------------------------------------------------------------- fake NEON client

@dataclass
class FakeEnvelope:
    data: Any
    status: int = 200
    headers: dict = field(default_factory=dict)


@dataclass
class FakeClient:
    """post_json/get_json/stream_to_file with scripted responses and failures."""

    responses: dict[str, Any] = field(default_factory=dict)  # key -> payload (see key())
    blobs: dict[str, bytes] = field(default_factory=dict)  # url -> body
    post_failures: dict[str, list[Exception]] = field(default_factory=dict)
    stream_failures: dict[str, list[Exception]] = field(default_factory=dict)  # file name -> errors
    calls: list[tuple[str, Any]] = field(default_factory=list)
    url_version: int = 0

    @staticmethod
    def key(body: dict) -> str:
        return f"{body['productCode']}|{body['startDateMonth']}|{body['endDateMonth']}|{body.get('release') or ''}"

    async def post_json(self, path: str, body: dict, *, token: Any, timeout_s: float | None = None) -> FakeEnvelope:
        k = self.key(body)
        self.calls.append(("post", k))
        fails = self.post_failures.get(k) or []
        if fails:
            raise fails.pop(0)
        payload = self.responses.get(k)
        if payload is None:
            payload = {"releases": []}
        self.url_version += 1
        out = json.loads(json.dumps(payload))
        for r in out.get("releases") or []:
            for pk in r.get("packages") or []:
                for f in pk.get("files") or []:
                    if f.get("url"):
                        f["url"] = f["url"] + f"?X-Goog-Signature=v{self.url_version}"
        return FakeEnvelope(out)

    async def get_json(self, path: str, params: Any = None, *, token: Any, timeout_s: float | None = None) -> Any:
        self.calls.append(("get", path))
        return FakeEnvelope(self.responses.get(path, {}))

    async def stream_to_file(self, url: str, dest: Path, *, token: Any, max_bytes: int,
                             expected_md5: str | None) -> Any:
        from neon_mcp.errors import ToolError
        from neon_mcp.neon.client import StreamOutcome

        name = dest.name
        self.calls.append(("stream", name))
        assert token is None
        fails = self.stream_failures.get(name) or []
        if fails:
            raise fails.pop(0)
        body = self.blobs[url.split("?")[0]]
        if len(body) > max_bytes:
            raise ToolError("download_limit_exceeded", "too big")
        got = md5(body)
        if expected_md5 and got != expected_md5:
            raise ToolError("checksum_mismatch", "bad md5", details={"expected": expected_md5, "actual": got})
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        part.write_bytes(body)
        os.replace(part, dest)
        return StreamOutcome(path=dest, bytes=len(body), md5=got,
                             md5_verified=(got == expected_md5) if expected_md5 else None, seconds=0.0)

    async def aclose(self) -> None:
        return None


@dataclass
class FakeBundle:
    list_client: FakeClient
    download_client: FakeClient
    token: Any = "tok"

    def client(self, role: str) -> FakeClient:
        return {"list": self.list_client, "download": self.download_client}[role]

    async def aclose(self) -> None:
        return None


def neon_file(name: str, body: bytes, *, size: bool = True, md5_ok: bool = True) -> dict[str, Any]:
    return {"name": name, "size": len(body) if size else None, "md5": md5(body) if md5_ok else md5(b"other"),
            "crc32c": None, "url": f"https://storage.googleapis.com/bucket/{name}"}


def query_payload(product: str, releases: list[tuple[str, str | None, list[tuple[str, list[dict]]]]]) -> dict:
    """releases: [(tag, generationDate, [(month, files)])]."""
    return {"productCode": product, "releases": [
        {"release": tag, "generationDate": gen, "packages": [
            {"domainCode": "D99", "siteCode": SITE, "month": m, "packageType": "basic",
             "generationDate": "2025-01-01T00:00:00Z", "files": files} for m, files in pkgs]}
        for tag, gen, pkgs in releases]}
