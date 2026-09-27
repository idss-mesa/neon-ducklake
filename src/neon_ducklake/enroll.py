"""``neon-ducklake enroll``: create the iRODS project root and enroll it in MESA (idempotent).

Runs mesa-mcp's ``handle_mesa_ducklake_init_project`` in-process with this process's single
``DuckLakeClient`` installed as mesa-mcp's default client — never through the MCP tool, which would
leave the server holding the catalog's single-writer lock. Takes the global ``mesa-tag`` lock and
refuses to run while another process holds ``~/.mesa/ducklake/catalog.duckdb``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from . import env

log = logging.getLogger("neon_ducklake.enroll")


def nested_mesa_enabled(session: Any, root: str) -> list[str]:
    """Collections strictly below ``root`` that carry ``mesa.enabled`` (must be none)."""
    from irods.column import Like
    from irods.models import Collection, CollectionMeta

    q = session.query(Collection.name, CollectionMeta.value).filter(
        CollectionMeta.name == "mesa.enabled").filter(Like(Collection.name, root.rstrip("/") + "/%"))
    return sorted({r[Collection.name] for r in q})


def ensure_root(session: Any, root: str) -> bool:
    """Create ``root`` if missing; returns True when it was created."""
    from irods.exception import CollectionDoesNotExist

    try:
        session.collections.get(root)
        return False
    except CollectionDoesNotExist:
        session.collections.create(root, recurse=True)
        log.info("created %s", root)
        return True


def main() -> int:
    from mesa_mcp.ducklake.client import set_default_client
    from mesa_mcp.ducklake.tools.init_project import InitProjectInput, handle_mesa_ducklake_init_project

    m = env.mesa()
    created = ensure_root(m.session, env.IRODS_ROOT)
    with env.file_lock("mesa-tag"):
        holders = env.catalog_lock_holders()
        if holders:
            for h in holders:
                print(f"catalog held by PID {h['pid']}: {h['cmd'][:120]}")
            raise SystemExit("close the process(es) holding ~/.mesa/ducklake/catalog.duckdb, then re-run enroll")
        try:
            client = env.mesa_ducklake_client(m)
            set_default_client(client)  # (mesa_ducklake_client already does; explicit for clarity)
            res = asyncio.run(handle_mesa_ducklake_init_project(InitProjectInput(irods_path=env.IRODS_ROOT),
                                                                auth_value=m.auth))
            project = client.find_project_by_path(env.IRODS_ROOT)
            if project is None:
                raise SystemExit(f"init_project returned but {env.IRODS_ROOT} is not in the MESA catalog")
        finally:
            env.close_mesa_ducklake_client()
    nested = nested_mesa_enabled(m.session, env.IRODS_ROOT)
    if nested:
        print("ERROR: nested mesa.enabled below the project root (one project only):")
        for p in nested:
            print(f"  {p}")
        return 1
    print(json.dumps({"root_created": created, "init_project": res,
                      "project": project.model_dump(mode="json")}, indent=1, default=str))
    return 0
