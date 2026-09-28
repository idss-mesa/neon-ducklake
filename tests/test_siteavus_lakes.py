"""site/avus.py + verify_site.py readers against real (tiny) DuckLake lakes and ontology store in tmp_path."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from neon_ducklake import env, state
from neon_ducklake.avu import bulk
from neon_ducklake.site import avus, ontology_store, site_lake
from neon_ducklake.verify_site import load_site_data

from .test_site_common import site_env  # noqa: F401 - fixture
from .test_siteavus_common import P_EXT, P_IS, P_OS, SITE, detach_site_avus, make_facts, write_local_files

detach_site_avus()  # see test_siteavus_common.detach_site_avus

S = f"/zone/home/u/proj/sites/{SITE}"


def _register(con: duckdb.DuckDBPyConnection, name: str, rows: list[dict[str, Any]]) -> None:
    import pyarrow as pa

    con.register(name, pa.Table.from_pylist(rows))


def make_site_lake(facts) -> None:
    con = site_lake.open_site_lake(SITE)
    try:
        tables: dict[str, list[dict[str, Any]]] = {
            "files": [{c: r.get(c) for c in site_lake.FILES_COLUMNS} for r in facts.files],
            "site_months": [{c: r.get(c) for c in site_lake.SITE_MONTHS_COLUMNS} for r in facts.site_months],
            "file_stats": list(facts.file_stats.values()),
            "file_taxa": [t for ts in facts.file_taxa.values() for t in ts],
            "sensor_positions": [r for rows in facts.sensor_positions.values() for r in rows],
            "h5_attrs": [{"productCode": "DP4.00200.001", "sourceFile": k, "month": "2024-06", "path": f"/{SITE}",
                          "attr": a, "value": v} for k, (lat, lon) in facts.h5_tower.items()
                         for a, v in (("LatTow", lat), ("LonTow", lon))],
            "table_catalog": [{"tableName": t, "productCode": p, "neonTable": t.split("__")[-1], "kind": "data"}
                              for p, ts in facts.lake_tables.items() for t in ts],
            "variables": [v for vs in facts.variables.values() for v in vs],
        }
        with site_lake.commit(con, "test fixture"):
            for name, rows in tables.items():
                _register(con, f"src_{name}", rows)
                con.execute(f"CREATE TABLE lake.{name} AS SELECT * FROM src_{name}")
            _register(con, "src_links", [{c: r.get(c) for c in site_lake.EXTERNAL_LINKS_COLUMNS}
                                         for r in facts.external_links])
            con.execute("CREATE TABLE lake.external_links AS SELECT * FROM src_links")
        site_lake.checkpoint_detach(con, SITE)
    finally:
        con.close()


def make_catalog_lake() -> None:
    env.LAKE_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("LOAD ducklake")
    con.execute(f"ATTACH 'ducklake:{env.LAKE_DIR / env.CATALOG_LAKE}' AS neon (DATA_PATH '{env.LAKE_DIR / 'data'}/')")
    con.execute(f"""CREATE TABLE neon.sites AS SELECT '{SITE}' AS siteCode, 'Test Site NEON' AS siteName,
        'Test Site NEON' AS siteDescription, 'CORE' AS siteType, 31.91068 AS siteLatitude, -110.83549 AS siteLongitude,
        'AZ' AS stateCode, 'Arizona' AS stateName, 'D99' AS domainCode, 'Test Domain' AS domainName,
        'https://deims.org/test' AS deimsId, 997.476 AS siteElevation,
        MAP {{'TypeEco': 'Desert shrubland', 'County': 'Pima'}} AS siteProperties""")
    con.execute(f"""CREATE TABLE neon.products AS SELECT * FROM (VALUES
        ('{P_IS}', 'Single aspirated air temperature', 'desc', 'SAAT abstract.', 'design', 'ACTIVE',
         'Level 1 Data Product', 'Terrestrial Instrument System (TIS)', 'TIS', ['Atmosphere'], ['air temperature']),
        ('{P_OS}', 'Breeding landbird point counts', 'desc', 'Birds.', NULL, 'ACTIVE', 'Level 1 Data Product',
         'Terrestrial Observation System (TOS)', 'TOS', ['Organisms, Populations, and Communities'], ['birds']))
        t(productCode, productName, productDescription, productAbstract, productDesignDescription, productStatus,
          productCategory, productScienceTeam, productScienceTeamAbbr, themes, keywords)""")
    con.execute(f"""CREATE TABLE neon.product_releases AS SELECT * FROM (VALUES
        ('{P_IS}', 'RELEASE-2026', '10.48443/p69b-5e50'), ('{P_OS}', 'RELEASE-2026', 'https://doi.org/10.48443/v6hs-mx57'))
        t(productCode, release, productDoi)""")
    con.execute(f"""CREATE TABLE neon.product_specs AS SELECT '{P_IS}' AS productCode, 'NEON.DOC.1' AS specNumber,
        'ATBD' AS specDescription, 'https://data.neonscience.org/api/v0/documents/NEON.DOC.1' AS specUrl""")
    con.execute(f"""CREATE TABLE neon.neon_tos_nlcdcoverbysite AS SELECT * FROM (VALUES
        ('D99', '{SITE}', '2006', 'shrubScrub', 99.0), ('D99', '{SITE}', '2006', 'evergreenForest', 1.0),
        ('D01', 'HARV', '2006', 'deciduousForest', 16.0)) t(domainID, siteID, nlcdYear, nlcdClass, area_km2)""")
    con.execute("""CREATE TABLE neon.taxonomy AS SELECT * FROM (VALUES ('BIRD', 'BTSP', 'Amphispiza bilineata'),
        ('BIRD', 'UNKN', 'Unknownus avis'), ('PLANT', 'OTHER', 'Plantus')) t(taxonTypeCode, taxonID, scientificName)""")
    con.execute(f"""CREATE TABLE neon.v_site_inventory AS SELECT * FROM (VALUES
        ('{P_IS}', '{SITE}', '2024-06', 'RELEASE-2026', false, false, false, 6, 400),
        ('{P_IS}', '{SITE}', '2024-07', 'RELEASE-2026', false, false, false, 6, 400),
        ('{P_EXT}', '{SITE}', '2024-06', 'PROVISIONAL', true, false, true, 0, 0))
        t(productCode, siteCode, month, release, isProvisional, isReleaseOnly, isExternal, fileCount, totalBytes)""")
    con.execute("DETACH neon")
    con.close()


def make_store() -> None:
    with ontology_store.store() as con:
        con.execute("INSERT INTO ref_ncbitaxon_map (taxonTypeCode, taxonID, neonName, curie, label, method, status) "
                    "VALUES ('BIRD', 'BTSP', 'Amphispiza bilineata', 'NCBITaxon:198939', 'Amphispiza bilineata', "
                    "'exact', 'ok'), ('BIRD', 'UNKN', 'Unknownus avis', NULL, NULL, 'unresolved', 'unresolved')")
        con.execute("INSERT INTO ref_unit_map (neonUnit, uoCurie, uoLabel, method, status) VALUES "
                    "('celsius', 'UO:0000027', 'degree Celsius', 'seed', 'ok')")
        con.execute("INSERT INTO ref_nlcd_envo VALUES ('shrubScrub', 'ENVO:01000869', 'area of scrub')")
        con.execute(f"INSERT INTO ref_site_envo VALUES ('{SITE}', 'ENVO:01000218', 'xeric shrubland biome', "
                    "'Desert shrubland', 'heuristic', 'proposed')")
        con.execute(f"INSERT INTO curation_avus (proposalId, productCode, attribute, value, unit, curie, label, "
                    f"ontologyId, status) VALUES ('p1', '{P_IS}', 'envo.air', 'SAAT_30min.tempSingleMean', "
                    f"'ENVO:00002005', 'ENVO:00002005', 'air', 'envo', 'accepted'), ('p2', '{P_IS}', 'envo.water', "
                    f"'x', 'ENVO:00002006', 'ENVO:00002006', 'water', 'envo', 'proposed')")


def make_journal(facts) -> None:
    con = state.connect(state.DB_PATH)
    try:
        con.executemany(
            "INSERT INTO site_files(site, product, month, name, kind, size, md5, dl_status, dl_md5, dl_bytes, "
            "md5_verified, downloaded_at, local_path) VALUES (?,?,?,?,?,?,?, 'ok', ?,?,?,?,?)",
            [(SITE, r["productCode"], r["month"], r["name"], r["kind"], r["size"], r["md5"], r["dlMd5"],
              (env.ROOT / r["localRelPath"]).stat().st_size, 1, "2026-09-27T14:59:42+00:00", r["localRelPath"])
             for r in facts.files])
        con.execute("INSERT INTO uploads(local_path, irods_path, size, md5, status) VALUES (?,?,?,?, 'present')",
                    (str(env.ROOT / "ref/ontology_maps/ref_unit_map.parquet"),
                     f"{env.IRODS_ROOT}/ref/ontology_maps/ref_unit_map.parquet", 10, "x"))
    finally:
        con.close()


@pytest.fixture
def lakes(site_env: Path):  # noqa: F811
    facts = make_facts()
    write_local_files(facts)
    make_site_lake(facts)
    make_catalog_lake()
    make_store()
    make_journal(facts)
    return facts


def test_load_facts_reads_every_source(lakes) -> None:
    f = avus.load_facts(SITE)
    assert f.catalog_ok and f.site_info["typeEco"] == "Desert shrubland"
    assert f.site_info["siteProperties"]["County"] == "Pima"
    assert set(f.products) == {P_IS, P_OS}
    assert f.dois == {(P_IS, "RELEASE-2026"): "10.48443/p69b-5e50", (P_OS, "RELEASE-2026"): "10.48443/v6hs-mx57"}
    assert f.specs[P_IS][0]["specNumber"] == "NEON.DOC.1"
    assert [r["nlcdClass"] for r in f.nlcd] == ["shrubScrub", "evergreenForest"]
    assert f.taxon_types == {"BTSP": [("BIRD", "Amphispiza bilineata")], "UNKN": [("BIRD", "Unknownus avis")]}
    assert len(f.files) == len(lakes.files) and set(f.file_stats) == set(lakes.file_stats)
    assert set(f.sensor_positions) == set(lakes.sensor_positions) and f.sensor_counts == lakes.sensor_counts
    assert f.h5_tower == lakes.h5_tower
    assert sorted(f.lake_tables[P_IS]) == sorted(lakes.lake_tables[P_IS])
    assert (P_IS, "2024-06", lakes.files[0]["name"]) in f.downloads
    assert f.uploaded_ref == {f"{env.IRODS_ROOT}/ref/ontology_maps/ref_unit_map.parquet"}
    o = f.ontology
    assert {"ref_ncbitaxon_map", "ref_unit_map", "ref_nlcd_envo", "ref_site_envo", "curation_avus"} <= o.tables
    assert [r["attribute"] for r in o.curated[P_IS]] == ["envo.air"]  # proposed rows are not applied
    assert o.site_envo["status"] == "proposed"


def test_build_from_lakes(lakes) -> None:
    con = state.connect(state.DB_PATH)
    s = avus.build_desired(SITE, con=con)
    assert s["catalog"] and s["rows"] > 200 and "datacite_errors" not in s
    site = {(a, v, u) for a, v, u in con.execute("SELECT attribute, value, unit FROM avu_desired WHERE path=?", (S,))}
    assert ("neon.typeEco", "Desert shrubland", "") in site
    assert ("envo.area_of_scrub", "NLCD 2006 shrubScrub 99.00 km2 (99.0%)", "ENVO:01000869") in site
    assert not any(a == "envo.xeric_shrubland_biome" for a, _v, _u in site)  # biome only when curated
    ref = con.execute("SELECT level, count(*) FROM avu_desired WHERE path LIKE ? GROUP BY 1",
                      (f"{env.IRODS_ROOT}/ref%",)).fetchall()
    assert {r[0] for r in ref} == {"container", "ref-file"}
    prod = {(a, v) for a, v in con.execute("SELECT attribute, value FROM avu_desired WHERE path=?", (f"{S}/raw/{P_IS}",))}
    assert ("envo.air", "SAAT_30min.tempSingleMean") in prod and ("uo.degree_celsius", "SAAT_30min.tempSingleMean") in prod
    assert not any(a == "envo.water" for a, _v in prod)
    con.close()


def test_verify_site_reader(lakes) -> None:
    d = load_site_data(SITE)
    assert len(d.files) == len(lakes.files) and d.inventory is not None and len(d.inventory) == 3
    assert (P_IS, "2024-06", "000.010") in d.sensor_keys
    assert d.variables[(P_IS, "SAAT_30min", "tempSingleMean")] == "real"
    assert len(d.journal) == len(lakes.files)
    assert d.taxa == {(P_OS, "BTSP", "Amphispiza bilineata"), (P_OS, "UNKN", "Unknownus avis")}


def test_missing_site_lake_is_a_clear_error(site_env: Path) -> None:  # noqa: F811
    with pytest.raises(SystemExit, match="site lake"):
        avus.load_facts(SITE)


def test_catalog_missing_degrades(site_env: Path) -> None:  # noqa: F811
    facts = make_facts()
    write_local_files(facts)
    make_site_lake(facts)
    f = avus.load_facts(SITE)
    assert not f.catalog_ok and f.products == {} and f.site_info == {}
    con = state.connect(state.DB_PATH)
    bulk.ensure_desired(con)
    s = avus.build_desired(SITE, con=con, facts=f)
    assert s["rows"] > 100 and not s["catalog"]
    assert con.execute("SELECT count(*) FROM avu_desired WHERE path=? AND attribute='neon.siteCode'",
                       (S,)).fetchone()[0] == 1
    con.close()


def test_timestamps_roundtrip_utc(lakes) -> None:
    f = avus.load_facts(SITE)
    sm = next(r for r in f.site_months if r["productCode"] == P_IS and r["month"] == "2024-06")
    assert avus.iso(sm["packageGenerationDate"]) == "2025-01-29T00:07:30Z"
    assert avus.as_utc(datetime(2024, 1, 1, tzinfo=UTC)) == datetime(2024, 1, 1, tzinfo=UTC)
