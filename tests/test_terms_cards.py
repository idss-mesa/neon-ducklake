"""site/cards.py on an in-memory site lake + catalog: product cards, anyjev DatasetCards, real profiles."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import duckdb
import numpy as np
import pytest

from neon_ducklake import env
from neon_ducklake.site import cards, site_lake
from neon_ducklake.site import ontology_store as store

from .test_site_common import SITE, site_env  # noqa: F401 - fixture

IS, OS, EC, EXT = "DP1.00002.001", "DP1.10003.001", "DP4.00200.001", "DP1.00033.001"
LONG = "Sentence one is here. " * 120  # ~2,600 chars


def _objdesc_h5(path: Path) -> None:
    import h5py

    path.parent.mkdir(parents=True, exist_ok=True)
    dt = np.dtype([("Object", "S22"), ("Description", "S120")])
    rows = [(b"fluxCo2", b"CO2 flux (can be turbulent, storage, or nsae)"), (b"turb", b"Turbulent"),
            (b"qfFinl", b"The final quality flag (1=fail, 0=pass)"), (b"veloFric", b"Friction velocity"),
            (b"timeBgn", b"The beginning time of the aggregation period")]
    with h5py.File(path, "w") as f:
        f.create_dataset("objDesc", data=np.array(rows, dtype=dt))


def build(root: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("ATTACH ':memory:' AS lake")
    con.execute("ATTACH ':memory:' AS neon")
    con.execute("""CREATE TABLE neon.products AS SELECT * FROM (VALUES
        ('DP1.00002.001', 'Single aspirated air temperature', 'Air temperature, one- and thirty-minute averages',
         'The abstract. ' || repeat('Long text here. ', 150), 'Design text.', 'Terrestrial Instrument System (TIS)',
         'TIS', 'ACTIVE', ['Atmosphere'], ['air temperature', 'tower'], 'RELEASE-2026', '10.48443/abcd', 'PRT'),
        ('DP1.10003.001', 'Breeding landbird point counts', 'Count of breeding landbirds', 'Birds.', 'Grids.',
         'Terrestrial Observation System (TOS)', 'TOS', 'ACTIVE', ['Organisms'], ['birds'], NULL, NULL, NULL),
        ('DP4.00200.001', 'Bundled data products - eddy covariance', 'Eddy covariance bundle', 'EC.', 'Tower.',
         'Terrestrial Instrument System (TIS)', 'TIS', 'ACTIVE', ['Atmosphere'], ['flux'], NULL, NULL, NULL),
        ('DP1.00033.001', 'Phenology images', 'Phenocam images', 'Images.', 'Cameras.',
         'Terrestrial Instrument System (TIS)', 'TIS', 'ACTIVE', [], [], NULL, NULL, NULL))
        t(productCode, productName, productDescription, productAbstract, productDesignDescription,
          productScienceTeam, productScienceTeamAbbr, productStatus, themes, keywords, latestRelease,
          latestReleaseDoi, productSensor)""")
    con.execute("CREATE TABLE neon.sites (siteCode VARCHAR, siteName VARCHAR, stateCode VARCHAR, domainCode VARCHAR, "
                "domainName VARCHAR, siteProperties MAP(VARCHAR, VARCHAR))")
    con.execute("INSERT INTO neon.sites VALUES ('TEST', 'Test Range, NEON', 'AZ', 'D14', 'Desert Southwest', "
                "MAP {'TypeEco': 'Desert shrubland (creosote)'})")
    con.execute("CREATE TABLE lake.site_months (productCode VARCHAR, month VARCHAR, release VARCHAR, "
                "isProvisional BOOLEAN, isReleaseOnly BOOLEAN, isExternal BOOLEAN)")
    con.executemany("INSERT INTO lake.site_months VALUES (?, ?, ?, ?, ?, ?)", [
        (IS, "2024-06", "RELEASE-2026", False, False, False), (IS, "2026-08", "PROVISIONAL", True, False, False),
        (OS, "2024-05", "RELEASE-2026", False, False, False), (EC, "2024-06", "RELEASE-2026", False, False, False),
        (EXT, "2024-06", "PROVISIONAL", True, False, True)])
    tc = site_lake.STACK_SCHEMAS["table_catalog"]
    con.execute("CREATE TABLE lake.table_catalog (" + ", ".join(f'"{c}" {t}' for c, t in tc.items()) + ")")
    for row in [("dp1_00002_001__saat_30min", IS, "SAAT_30min", "data", 2, 4), ("variables", IS, "variables",
                "support", 2, 3), ("dp1_10003_001__brd_countdata", OS, "brd_countdata", "data", 1, 6),
                ("dp4_00200_001__nsae_dp04", EC, "nsae", "h5", 1, 3)]:
        con.execute("INSERT INTO lake.table_catalog (tableName, productCode, neonTable, kind, nFiles, nRows) "
                    "VALUES (?, ?, ?, ?, ?, ?)", list(row))
    con.execute("CREATE TABLE lake.variables (productCode VARCHAR, tableName VARCHAR, fieldName VARCHAR, "
                "description VARCHAR, dataType VARCHAR, units VARCHAR)")
    con.executemany("INSERT INTO lake.variables VALUES (?, ?, ?, ?, ?, ?)", [
        (IS, "SAAT_30min", "startDateTime", "Date and time at which a sampling is initiated", "dateTime", None),
        (IS, "SAAT_30min", "tempSingleMean", "Arithmetic mean | of single aspirated air temperature", "real", "celsius"),
        (IS, "SAAT_30min", "finalQF", "Quality flag", "unsigned integer", "NA"),
        (IS, "SAAT_30min", "siteID", "NEON site code", "string", None),
        (OS, "brd_countdata", "taxonID", "Species code", "string", None),
        (OS, "brd_countdata", "remarks", "Technician notes", "string", None),
        (OS, "brd_countdata", "clusterSize", "Number of individuals", "unsigned integer", "number")])
    ts = [datetime(2024, 6, 1, 0, 0, tzinfo=UTC), datetime(2024, 6, 1, 0, 30, tzinfo=UTC),
          datetime(2026, 8, 1, 0, 0, tzinfo=UTC), datetime(2026, 8, 1, 0, 30, tzinfo=UTC)]
    con.execute("CREATE TABLE lake.dp1_00002_001__saat_30min (startDateTime TIMESTAMPTZ, tempSingleMean DOUBLE, "
                "finalQF BIGINT, siteID VARCHAR, month VARCHAR, release VARCHAR, horizontalPosition VARCHAR)")
    con.executemany("INSERT INTO lake.dp1_00002_001__saat_30min VALUES (?, ?, ?, 'TEST', ?, ?, '000')", [
        (ts[0], 20.5, 0, "2024-06", "RELEASE-2026"), (ts[1], 21.5, 0, "2024-06", "RELEASE-2026"),
        (ts[2], 30.0, 1, "2026-08", "PROVISIONAL"), (ts[3], None, 0, "2026-08", "PROVISIONAL")])
    con.execute("CREATE TABLE lake.dp1_10003_001__brd_countdata (taxonID VARCHAR, remarks VARCHAR, "
                "clusterSize BIGINT, uid VARCHAR, month VARCHAR)")
    con.executemany("INSERT INTO lake.dp1_10003_001__brd_countdata VALUES (?, NULL, ?, ?, '2024-05')", [
        ("CACW", 1, "u1"), ("CACW", 2, "u2"), ("GAQU", 1, "u3"), ("GAQU", 1, "u4"), ("GAQU", 3, "u5"),
        ("ZZZZ", 1, "u6")])
    con.execute("CREATE TABLE lake.dp4_00200_001__nsae_dp04 (timeBgn TIMESTAMPTZ, fluxCo2Turb DOUBLE, "
                "qfFinlCo2Turb BIGINT, footVeloFric DOUBLE, month VARCHAR)")
    con.executemany("INSERT INTO lake.dp4_00200_001__nsae_dp04 VALUES (?, ?, ?, ?, '2024-06')", [
        (ts[0], 1.5, 0, 0.3), (ts[1], -2.0, 1, 0.4), (ts[2], None, 0, 0.5)])
    con.execute("CREATE TABLE lake.h5_datasets (productCode VARCHAR, path VARCHAR, fields VARCHAR[], unit VARCHAR)")
    con.executemany("INSERT INTO lake.h5_datasets VALUES (?, ?, ?, ?)", [
        (EC, "/TEST/dp04/data/fluxCo2/turb", ["timeBgn", "timeEnd", "flux"], "NA,NA,umolCo2 m-2 s-1"),
        (EC, "/TEST/dp04/qfqm/fluxCo2/turb", ["timeBgn", "timeEnd", "qfFinl"], "NA,NA,NA"),
        (EC, "/TEST/dp04/data/foot/stat", ["timeBgn", "timeEnd", "veloFric"], "NA,NA,m s-1")])
    rel = f"sites/{SITE}/raw/{EC}/2024-06/NEON.D14.TEST.DP4.00200.001.nsae.2024-06.basic.20260115T171049Z.h5"
    _objdesc_h5(root / rel)
    con.execute("CREATE TABLE lake.files (productCode VARCHAR, month VARCHAR, name VARCHAR, localRelPath VARCHAR)")
    con.execute("INSERT INTO lake.files VALUES (?, '2024-06', ?, ?)", [EC, Path(rel).name, rel])
    fs = site_lake.STACK_SCHEMAS["file_stats"]
    con.execute("CREATE TABLE lake.file_stats (" + ", ".join(f'"{c}" {t}' for c, t in fs.items()) + ")")
    con.execute("INSERT INTO lake.file_stats (sourceFile, productCode, lakeTable, hor, ver) VALUES "
                "('f1', ?, 'dp1_00002_001__saat_30min', '000', '010')", [IS])
    ft = site_lake.STACK_SCHEMAS["file_taxa"]
    con.execute("CREATE TABLE lake.file_taxa (" + ", ".join(f'"{c}" {t}' for c, t in ft.items()) + ")")
    con.executemany("INSERT INTO lake.file_taxa VALUES ('f', ?, 'brd_countdata', ?, ?, 'species', ?, ?)", [
        (OS, "CACW", "Campylorhynchus brunneicapillus", "Cactus Wren", 2),
        (OS, "GAQU", "Callipepla gambelii", "Gambel's Quail", 3), (OS, "ZZZZ", "Unknownus", None, 1)])
    con.execute("CREATE TABLE lake.external_links (productCode VARCHAR, url VARCHAR, name VARCHAR, type VARCHAR, "
                "monthProbed VARCHAR, months VARCHAR[])")
    con.execute("INSERT INTO lake.external_links VALUES (?, 'https://phenocam.example/TEST/', 'Phenocam', "
                "'Phenocam Website', '2026-09', ['2017-02', '2026-09'])", [EXT])
    return con


@pytest.fixture
def lakes(site_env: Path) -> duckdb.DuckDBPyConnection:  # noqa: F811
    with store.store() as sc:
        sc.execute("INSERT INTO ref_unit_map VALUES ('celsius', 'UO:0000027', 'degree Celsius', 'seed', 'ok', now())")
        sc.executemany("INSERT INTO ref_ncbitaxon_map VALUES ('BIRD', ?, ?, ?, ?, ?, ?, ?, now())", [
            ("CACW", "Campylorhynchus brunneicapillus", "Campylorhynchus brunneicapillus", "NCBITaxon:141853",
             "Campylorhynchus brunneicapillus", "exact", "ok"),
            ("GAQU", "Callipepla gambelii", "Callipepla gambelii", "NCBITaxon:67773", "Callipepla gambelii",
             "exact", "ok"),
            ("ZZZZ", "Unknownus", "Unknownus", None, None, "unresolved", "unresolved")])
        sc.execute("INSERT INTO terms_taxon_types VALUES (?, ?, 'brd_countdata', 'BIRD', 3, 3, 1.0, [], now())",
                   [SITE, OS])
    return build(site_env)


def test_cards_written_with_real_profiles(lakes):
    summary = cards.build_cards(lakes, SITE, None, True)
    assert summary["problems"] == {}
    card_dir = env.SITES_DIR / SITE / "cards"
    assert sorted(p.name for p in card_dir.glob("*.md")) == [f"{p}.md" for p in sorted([IS, EXT, OS, EC])]
    assert sorted(p.name for p in (card_dir / "anyjev").glob("*.md")) == [
        f"{IS}.SAAT_30min.md", f"{OS}.brd_countdata.md", f"{EC}.nsae_dp04.md"]
    text = (card_dir / f"{IS}.md").read_text()
    assert "not profiled" not in text
    assert "Site: TEST (Test Range NEON, AZ, domain D14 Desert Southwest; Desert shrubland creosote)" in text
    assert "Rows: 4; months covered: 2024-06 to 2026-08 (2 distinct months)." in text
    assert "Sensor positions (HOR.VER): 000.010 (1); files: 2" in text
    assert ("- tempSingleMean | Arithmetic mean / of single aspirated air temperature | real | celsius | "
            "numeric, n=3, 3 distinct, min=20.5, median=21.5, max=30; top: 20.5 (1), 21.5 (1), 30 (1); 1 blank") in text
    assert "- finalQF | Quality flag | unsigned integer | - | numeric, n=4, 2 distinct, min=0, median=0, max=1; " \
           "top: 0 (3), 1 (1)" in text
    assert ("- startDateTime | Date and time at which a sampling is initiated | dateTime | - | datetime, n=4, "
            "4 distinct, min=2024-06-01T00:00Z, max=2026-08-01T00:30Z") in text
    assert "- horizontalPosition | Horizontal (HOR) index" in text  # an added column without a variables row
    assert "- celsius -> UO:0000027 'degree Celsius' [seed, ok]" in text
    abstract = text.split("## Abstract (excerpt)\n", 1)[1].split("\n", 1)[0]
    assert len(abstract) <= cards.EXCERPT_CHARS and abstract.endswith(" …")
    assert "Latest release: RELEASE-2026 (doi:10.48443/abcd)" in text


def test_os_card_taxa_and_strings(lakes):
    cards.build_cards(lakes, SITE, [OS], True)
    text = (env.SITES_DIR / SITE / "cards" / f"{OS}.md").read_text()
    assert "- remarks | Technician notes | string | - | all blank" in text
    assert "- taxonID | Species code | string | - | 3 distinct; top: GAQU (3), CACW (2), ZZZZ (1)" in text
    assert "- uid | (no NEON description) | string | - | 6 distinct (all unique); e.g. u1, u2, u3, u4, u5" in text
    assert "NCBITaxon resolution: exact 2, unresolved 1" in text
    assert ("- Callipepla gambelii (GAQU; Gambel's Quail; species): 3 records -> NCBITaxon:67773 "
            "'Callipepla gambelii' [exact, ok]") in text
    assert "- Unknownus (ZZZZ; species): 1 records -> [unresolved, unresolved]" in text
    assert not (env.SITES_DIR / SITE / "cards" / f"{IS}.md").exists()  # product filter


def test_h5_card_uses_objdesc_and_units(lakes):
    cards.build_cards(lakes, SITE, [EC], True)
    text = (env.SITES_DIR / SITE / "cards" / "anyjev" / f"{EC}.nsae_dp04.md").read_text()
    assert text.startswith(f"# Dataset: {EC} / nsae_dp04\nProduct: {EC} — Bundled data products - eddy covariance. ")
    assert ("- fluxCo2Turb | CO2 flux (can be turbulent, storage, or nsae); level turb: Turbulent "
            "(HDF5 dp04/data/fluxCo2/turb, field flux) | real | umolCo2 m-2 s-1 | numeric") in text
    assert "- qfFinlCo2Turb | The final quality flag (1=fail, 0=pass) (fluxCo2 turb;" in text
    assert "- footVeloFric | Friction velocity (HDF5 dp04/data/foot/stat, field veloFric) | real | m s-1 |" in text
    assert "- timeBgn | The beginning time of the aggregation period | dateTime | - | datetime" in text
    product = (env.SITES_DIR / SITE / "cards" / f"{EC}.md").read_text()
    assert "- umolCo2 m-2 s-1 -> [HDF5 attribute unit" in product


def test_external_card(lakes):
    cards.build_cards(lakes, SITE, [EXT], True)
    text = (env.SITES_DIR / SITE / "cards" / f"{EXT}.md").read_text()
    assert "- Phenocam (Phenocam Website): https://phenocam.example/TEST/; months 2017-02 to 2026-09 (2)" in text
    assert "## Table" not in text
    assert not list((env.SITES_DIR / SITE / "cards" / "anyjev").glob(f"{EXT}.*"))


def test_anyjev_grammar_self_check(lakes):
    summary = cards.build_cards(lakes, SITE, None, True)
    for a in summary["anyjev"]:
        text = Path(a["path"]).read_text()
        assert cards.grammar_problems(text, a["columns"]) == []
        site_line = next(ln for ln in text.splitlines() if ln.startswith("Sites:"))
        m = next(cards._SITE.finditer(site_line))
        assert (m["code"], m["state"], m["domain"], m["habitat"]) == ("TEST", "AZ", "D14", "Desert shrubland creosote")
    assert cards.grammar_problems("# Dataset: X / y\nRows: 1; months covered: a to b\n- c | d | e | f | not profiled",
                                  1) == ["product line", "site line", "'not profiled'"]


@pytest.mark.skipif(not cards.ANYJEV_PYTHON.exists(), reason="mesa-anyjev venv not present")
def test_anyjev_parse_card_subprocess(lakes):
    summary = cards.build_cards(lakes, SITE, [OS], True)
    paths = [Path(a["path"]) for a in summary["anyjev"]]
    parsed = cards.anyjev_parse(paths)
    assert parsed is not None
    r = parsed[str(paths[0])]
    assert r["ok"] and r["columns"] == 5 and r["rows"] == 6 and r["sites"] == [SITE]
    assert (r["product"], r["table"], r["from"], r["to"]) == (OS, "brd_countdata", "2024-05", "2024-05")


def test_cards_idempotent(lakes):
    cards.build_cards(lakes, SITE, None, True)
    files = sorted((env.SITES_DIR / SITE / "cards").rglob("*.md"))
    before = {p: p.stat().st_mtime_ns for p in files}
    cards.build_cards(lakes, SITE, None, True)
    assert {p: p.stat().st_mtime_ns for p in files} == before


def test_helpers():
    assert cards.excerpt("short") == "short"
    long = cards.excerpt(LONG)
    assert len(long) <= 1500 and long.endswith("here. …")
    assert cards.fmt_num(3.0) == "3" and cards.fmt_num(0.1234567) == "0.123457" and cards.fmt_num(None) == "NA"
    assert cards.fmt_num(float("nan")) == "NaN"
    assert cards.fmt_time(datetime(2024, 6, 1, 0, 30, tzinfo=UTC)) == "2024-06-01T00:30Z"
    assert cards.neon_dtype("UBIGINT") == "unsigned integer" and cards.neon_dtype("BIGINT") == "signed integer"
    assert cards.clean("a |\n b") == "a / b"


LAB = "DP1.20093.001"
NAIVE_2025 = datetime(2025, 1, 2, tzinfo=UTC).replace(tzinfo=None)  # files.publicationTimestamp is TIMESTAMP


def _add_lab(con: duckdb.DuckDBPyConnection) -> None:
    """A product with a stacked lab table and its lab files (NEON.<Lab>.<table>.<ts>.csv)."""
    con.execute("INSERT INTO lake.site_months VALUES (?, '2024-06', 'RELEASE-2026', false, false, false)", [LAB])
    con.execute("INSERT INTO lake.table_catalog (tableName, productCode, neonTable, kind, nFiles, nRows) VALUES "
                "('dp1_20093_001__lab_swc_externallabsummarydata', ?, 'swc_externalLabSummaryData', 'lab', 2, 3)",
                [LAB])
    con.execute("CREATE TABLE lake.dp1_20093_001__lab_swc_externallabsummarydata (analyte VARCHAR, "
                "methodDetectionLimit DOUBLE, labName VARCHAR, publicationDate TIMESTAMPTZ, sourceFile VARCHAR)")
    con.executemany("INSERT INTO lake.dp1_20093_001__lab_swc_externallabsummarydata VALUES (?, ?, ?, ?, ?)", [
        ("Ca", 0.01, "Univ_of_Wyoming", datetime(2025, 1, 2, tzinfo=UTC), "f1"),
        ("Mg", 0.02, "Univ_of_Wyoming", datetime(2025, 1, 2, tzinfo=UTC), "f1"),
        ("Ca", 0.01, "Univ_of_Wyoming", datetime(2026, 1, 2, tzinfo=UTC), "f2")])
    con.executemany("INSERT INTO lake.variables VALUES (?, 'swc_externalLabSummaryData', ?, ?, ?, ?)", [
        (LAB, "analyte", "Chemical analyte", "string", None),
        (LAB, "methodDetectionLimit", "Method detection limit", "real", "milligramsPerLiter")])
    con.execute("ALTER TABLE lake.files ADD COLUMN kind VARCHAR")
    con.execute("ALTER TABLE lake.files ADD COLUMN publicationTimestamp TIMESTAMP")
    con.executemany("INSERT INTO lake.files VALUES (?, '2024-06', ?, NULL, ?, ?)", [
        (LAB, "NEON.Univ_of_Wyoming.swc_externalLabSummaryData.20250102T000000Z.csv", "lab",
         NAIVE_2025),
        (LAB, "NEON.Univ_of_Wyoming.swc_externalLabSummaryData.20260102T000000Z.csv", "other",  # pre-classifier
         NAIVE_2025.replace(year=2026)),
        (LAB, "NEON.D10.TEST.DP1.20093.001.variables.20250102T000000Z.csv", "variables", NAIVE_2025)])


def test_lab_tables_and_files_in_product_card(lakes):
    _add_lab(lakes)
    cards.build_cards(lakes, SITE, [LAB], True)
    text = (env.SITES_DIR / SITE / "cards" / f"{LAB}.md").read_text()
    assert ("## Lab table swc_externalLabSummaryData (lake table dp1_20093_001__lab_swc_externallabsummarydata; "
            "lab)") in text
    assert "Rows: 3 (distinct lab records); files: 2" in text
    assert "- analyte | Chemical analyte | string | - | 2 distinct; top: Ca (2), Mg (1)" in text
    assert "- labName | (no NEON description) | string | - | 1 distinct; top: Univ_of_Wyoming (3)" in text
    assert "- Univ_of_Wyoming: swc_externalLabSummaryData (2 files, published 2025-01-02 to 2026-01-02)" in text
    assert "- milligramsPerLiter -> [not mapped yet" in text
    assert not list((env.SITES_DIR / SITE / "cards" / "anyjev").glob(f"{LAB}.*"))  # lab tables: product card only
    assert cards.lab_parts("NEON.NEON_HQ.mct_taxonTableLabSummary.20251206T221502Z.csv") == \
        ("NEON_HQ", "mct_taxonTableLabSummary")
    assert cards.lab_parts("NEON.D10.TEST.DP1.20093.001.variables.20250102T000000Z.csv") is None


def _heuristic(site: str, rows: list[tuple[str, str, str, str]]) -> None:
    with store.store() as sc:
        sc.executemany("INSERT OR REPLACE INTO site_envo_proposals (siteCode, role, source, envoCurie, envoLabel, "
                       "rule) VALUES (?, ?, 'heuristic', ?, ?, ?)", [(site, *r) for r in rows])


def test_site_card_terrestrial(lakes):
    lakes.execute("CREATE TABLE neon.neon_tos_nlcdcoverbysite (siteID VARCHAR, nlcdYear VARCHAR, nlcdClass VARCHAR, "
                  "area_km2 DOUBLE)")
    lakes.executemany("INSERT INTO neon.neon_tos_nlcdcoverbysite VALUES ('TEST', '2019', ?, ?)",
                      [("shrubScrub", 9.0), ("barrenLand", 0.95), ("openWater", 0.05)])
    _heuristic(SITE, [("biome", "ENVO:01000218", "xeric shrubland biome", "desert+shrub")])
    path = cards.write_site_card(lakes, SITE, True)
    assert path == env.SITES_DIR / SITE / "cards" / "_site.md"
    text = path.read_text()
    assert text.startswith("# Site: TEST — Test Range, NEON\n")
    assert "HABITAT: not recorded" in text and "Domain: D14 Desert Southwest" in text
    assert "TypeEco (NEON's ecosystem type): Desert shrubland (creosote)" in text
    assert "NLCD land cover (2019, NEON TOS site area): shrubScrub 90.0%; barrenLand 9.5%" in text  # < 1 % dropped
    assert "Products: 4 (non-AOP, basic package); science teams: TIS 3, TOS 1" in text
    assert "- DP1.10003.001 Breeding landbird point counts" in text
    assert "- biome: ENVO:01000218 xeric shrubland biome (rule desert+shrub)" in text
    assert "environmental feature" not in text  # terrestrial: biome only


def test_site_card_aquatic_uses_habitat_never_type_eco(lakes):
    lakes.execute("DELETE FROM neon.sites")
    lakes.execute("INSERT INTO neon.sites VALUES ('TEST', 'Test Creek NEON', 'CO', 'D10', 'Central Plains', "
                  "MAP {'TypeEco': 'lightly grazed North American natural shortgrass steppe', "
                  "'HABITAT': 'Aquatic Wadeable Stream'})")
    _heuristic(SITE, [("biome", "ENVO:03605008", "freshwater stream biome", "aquatic:wadeable stream"),
                      ("feature", "ENVO:00000023", "stream", "aquatic:wadeable stream")])
    text = cards.site_card(lakes, SITE, True)
    assert "HABITAT: Aquatic Wadeable Stream" in text
    assert "steppe" not in text and "TypeEco" not in text and "NLCD" not in text
    assert "- biome: ENVO:03605008 freshwater stream biome (rule aquatic:wadeable stream)" in text
    assert "- environmental feature (water body): ENVO:00000023 stream (rule aquatic:wadeable stream)" in text
    assert cards.site_line(lakes, SITE, True) == "TEST (Test Creek NEON, CO, domain D10 Central Plains; " \
                                                 "Aquatic Wadeable Stream)"
    summary = cards.build_cards(lakes, SITE, [IS], True)
    assert "_site" not in {p["product"] for p in summary["products"]}
    anyjev = (env.SITES_DIR / SITE / "cards" / "anyjev" / f"{IS}.SAAT_30min.md").read_text()
    assert cards.grammar_problems(anyjev, 7) == [] and "Aquatic Wadeable Stream)" in anyjev
