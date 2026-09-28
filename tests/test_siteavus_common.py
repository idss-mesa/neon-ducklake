"""Synthetic site facts shared by the ``test_siteavus_*`` modules (no lakes, no network, no iRODS)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from neon_ducklake import env
from neon_ducklake.site.avus import Facts, Ontology

SITE = "TEST"
P_IS, P_OS, P_H5, P_RO, P_EXT = "DP1.00002.001", "DP1.10003.001", "DP4.00200.001", "DP1.00006.001", "DP1.00033.001"
DOIS = {(P_IS, "RELEASE-2026"): "10.48443/p69b-5e50", (P_IS, "RELEASE-2025"): "10.48443/hejx-aa82",
        (P_OS, "RELEASE-2026"): "10.48443/v6hs-mx57", (P_H5, "RELEASE-2026"): "10.48443/xymh-2v16",
        (P_RO, "RELEASE-2024"): "10.48443/rr7n-7d52"}
LONG_ABSTRACT = "Single aspirated air temperature is measured on the tower. " * 60  # > 2,000 bytes
TS = datetime(2024, 7, 6, 16, 23, 40, tzinfo=UTC).replace(tzinfo=None)  # naive, like DuckDB TIMESTAMP


def test_common_facts_build() -> None:
    f = make_facts()
    assert f.site == SITE and len(f.files) > 10


def md5(b: bytes) -> str:
    return hashlib.md5(b, usedforsecurity=False).hexdigest()


def fname(p: str, m: str, kind: str, table: str | None = None, hv: str | None = None) -> str:
    d = f"NEON.D99.{SITE}.{p}"
    if kind == "data" and table == "nsae":
        return f"{d}.nsae.{m}.basic.20260115T171049Z.h5"
    if kind == "data" and hv:
        return f"{d}.{hv}.030.{table}.{m}.basic.20240706T162340Z.csv"
    if kind == "data":
        return f"{d}.{table}.{m}.basic.20260106T223543Z.csv"
    if kind == "eml":
        return f"{d}.EML.{m.replace('-', '')}01-{m.replace('-', '')}28.20260123T000749Z.xml"
    if kind in ("categorical_codes", "validation"):
        return f"NEON.D99.{SITE}.DP0.{p[4:]}.{table}.20260106T223543Z.csv"
    return f"{d}.{kind}.20240706T162340Z.{'txt' if kind == 'readme' else 'csv'}"


def frow(p: str, m: str, kind: str, *, table: str | None = None, hv: str | None = None, release: str = "RELEASE-2026",
         size: int | None = 100, md5_ok: bool = True, release_only: bool = False) -> dict[str, Any]:
    name = fname(p, m, kind, table, hv)
    hor, ver = (hv.split(".") if hv else (None, None))
    ext = name.rsplit(".", 1)[-1]
    body = f"{name}\n".encode()
    return {"productCode": p, "siteCode": SITE, "domainCode": "D99", "month": m, "release": release,
            "isProvisional": release == "PROVISIONAL", "isReleaseOnly": release_only, "name": name, "size": size,
            "md5": md5(body), "crc32c": None, "kind": kind, "tableName": table, "hor": hor, "ver": ver,
            "tmi": "030" if hv else None, "publicationTimestamp": TS, "ext": ext,
            "localRelPath": f"sites/{SITE}/raw/{p}/{m}/{name}", "irodsPath": None, "dlMd5": md5(body),
            "md5Verified": md5_ok}


def make_files() -> list[dict[str, Any]]:
    rows = []
    for m, rel in (("2024-06", "RELEASE-2026"), ("2026-08", "PROVISIONAL")):
        rows += [frow(P_IS, m, "data", table="SAAT_30min", hv="000.010", release=rel),
                 frow(P_IS, m, "data", table="SAAT_30min", hv="000.020", release=rel),
                 frow(P_IS, m, "sensor_positions", release=rel),
                 frow(P_IS, m, "variables", release=rel),
                 frow(P_IS, m, "eml", release=rel, size=None, md5_ok=(m != "2024-06")),
                 frow(P_IS, m, "readme", release=rel, size=None)]
    rows += [frow(P_OS, "2024-05", "data", table="brd_countdata"),
             frow(P_OS, "2024-05", "data", table="brd_perpoint"),
             frow(P_OS, "2024-05", "categorical_codes", table="categoricalCodes"),
             frow(P_OS, "2024-05", "validation", table="validation"),
             frow(P_H5, "2024-06", "data", table="nsae", size=5000),
             frow(P_RO, "2019-05", "data", table="PRIPRE_30min", hv="900.000", release="RELEASE-2024",
                  release_only=True)]
    return rows


def stats(name: str, p: str, table: str, month: str, rows: int, **kw: Any) -> dict[str, Any]:
    y, mo = int(month[:4]), int(month[5:])
    base = {"sourceFile": name, "productCode": p, "tableName": table,
            "lakeTable": f"{p.lower().replace('.', '_')}__{table.lower()}", "month": month, "rowCount": rows,
            "timeMin": datetime(y, mo, 1, tzinfo=UTC), "timeMax": datetime(y, mo, 28, 23, 30, tzinfo=UTC),
            "timeColumn": "startDateTime", "qfFailPct": None, "plotIDs": None, "plotCount": None,
            "protocolVersions": None, "taxonCount": None, "hor": None, "ver": None, "tmi": None}
    base.update(kw)
    return base


def make_ontology() -> Ontology:
    return Ontology(
        tables={"ref_ncbitaxon_map", "ref_unit_map", "ref_nlcd_envo", "ref_site_envo", "curation_avus"},
        taxa={("BIRD", "BTSP"): {"taxonTypeCode": "BIRD", "taxonID": "BTSP", "neonName": "Amphispiza bilineata",
                                 "curie": "NCBITaxon:198939", "label": "Amphispiza bilineata", "method": "exact",
                                 "status": "ok"},
              ("BIRD", "UNKN"): {"taxonTypeCode": "BIRD", "taxonID": "UNKN", "neonName": "Unknownus avis",
                                 "curie": None, "label": None, "method": "unresolved", "status": "unresolved"}},
        units={"celsius": {"neonUnit": "celsius", "uoCurie": "UO:0000027", "uoLabel": "degree Celsius",
                           "status": "ok"},
               "percent": {"neonUnit": "percent", "uoCurie": "UO:0000187", "uoLabel": "percent", "status": "ok"},
               "celsiusSquared": {"neonUnit": "celsiusSquared", "uoCurie": None, "uoLabel": None,
                                  "status": "unmapped"}},
        nlcd={"shrubscrub": {"nlcdClass": "shrubScrub", "envoCurie": "ENVO:01000869", "envoLabel": "area of scrub"},
              "grasslandherbaceous": {"nlcdClass": "grasslandHerbaceous", "envoCurie": "ENVO:01000888",
                                      "envoLabel": "area of grassland"},
              "evergreenforest": {"nlcdClass": "evergreenForest", "envoCurie": "ENVO:01000843",
                                  "envoLabel": "area of evergreen forest"}},
        site_envo={"siteCode": SITE, "envoCurie": "ENVO:01000218", "envoLabel": "xeric shrubland biome",
                   "typeEco": "Desert shrubland", "method": "curated", "status": "ok"},
        curated={P_IS: [{"productCode": P_IS, "attribute": "envo.air", "value": "SAAT_30min.tempSingleMean",
                         "unit": "ENVO:00002005", "curie": "ENVO:00002005", "label": "air", "ontologyId": "envo",
                         "status": "accepted"}]})


def make_facts(ontology: Ontology | None = None) -> Facts:
    files = make_files()
    f = Facts(site=SITE, catalog_ok=True, neon_mcp_version="0.1.0")
    f.site_info = {"siteCode": SITE, "siteName": "Test Site NEON", "siteDescription": "Test Site NEON",
                   "siteType": "CORE", "domainCode": "D99", "domainName": "Test Domain", "stateCode": "AZ",
                   "stateName": "Arizona", "siteLatitude": 31.91068, "siteLongitude": -110.83549,
                   "siteElevation": 997.476, "deimsId": "https://deims.org/test", "typeEco": "Desert shrubland"}
    f.products = {
        P_IS: {"productCode": P_IS, "productName": "Single aspirated air temperature", "productAbstract": LONG_ABSTRACT,
               "productDesignDescription": "SAAT assemblies are deployed on the tower.", "productStatus": "ACTIVE",
               "productCategory": "Level 1 Data Product", "productScienceTeam": "Terrestrial Instrument System (TIS)",
               "productScienceTeamAbbr": "TIS", "themes": ["Atmosphere"], "keywords": ["air temperature", "tower"]},
        P_OS: {"productCode": P_OS, "productName": "Breeding landbird point counts", "productAbstract": "Birds.",
               "productStatus": "ACTIVE", "productCategory": "Level 1 Data Product",
               "productScienceTeam": "Terrestrial Observation System (TOS)", "productScienceTeamAbbr": "TOS",
               "themes": ["Organisms, Populations, and Communities"], "keywords": ["birds"]},
        P_H5: {"productCode": P_H5, "productName": "Bundled data products - eddy covariance",
               "productStatus": "ACTIVE", "productCategory": "Level 4 Data Product", "themes": ["Atmosphere"],
               "keywords": ["flux"], "productScienceTeam": "Terrestrial Instrument System (TIS)"},
        P_RO: {"productCode": P_RO, "productName": "Precipitation", "productStatus": "ACTIVE",
               "productCategory": "Level 1 Data Product", "themes": ["Atmosphere"], "keywords": ["rain"],
               "productScienceTeam": "Terrestrial Instrument System (TIS)"},
        P_EXT: {"productCode": P_EXT, "productName": "Phenology images", "productStatus": "ACTIVE",
                "productCategory": "Level 1 Data Product", "themes": ["Land Use, Land Cover, and Land Processes"],
                "keywords": ["phenocam"], "productScienceTeam": "Terrestrial Instrument System (TIS)"},
    }
    f.dois = dict(DOIS)
    f.specs = {P_IS: [{"productCode": P_IS, "specNumber": "NEON.DOC.000646vD", "specDescription": "ATBD SAAT",
                       "specUrl": "https://data.neonscience.org/api/v0/documents/NEON.DOC.000646vD"}]}
    f.nlcd = [{"nlcdYear": "2006", "nlcdClass": "shrubScrub", "area_km2": 98.0},
              {"nlcdYear": "2006", "nlcdClass": "grasslandHerbaceous", "area_km2": 1.5},
              {"nlcdYear": "2006", "nlcdClass": "evergreenForest", "area_km2": 0.5}]
    f.taxon_types = {"BTSP": [("BIRD", "Amphispiza bilineata")], "UNKN": [("BIRD", "Unknownus avis")]}
    f.files = files
    f.site_months = [
        {"productCode": P_IS, "month": "2024-06", "release": "RELEASE-2026", "isProvisional": False,
         "isReleaseOnly": False, "releaseGenerationDate": datetime(2026, 1, 23, tzinfo=UTC),
         "packageGenerationDate": datetime(2025, 1, 29, 0, 7, 30, tzinfo=UTC), "fileCount": 6, "totalBytes": 400,
         "isExternal": False},
        {"productCode": P_IS, "month": "2026-08", "release": "PROVISIONAL", "isProvisional": True,
         "isReleaseOnly": False, "releaseGenerationDate": None,
         "packageGenerationDate": datetime(2026, 9, 7, 19, 10, 4, tzinfo=UTC), "fileCount": 6, "totalBytes": 400,
         "isExternal": False},
        {"productCode": P_OS, "month": "2024-05", "release": "RELEASE-2026", "isProvisional": False,
         "isReleaseOnly": False, "fileCount": 4, "totalBytes": 400, "isExternal": False},
        {"productCode": P_H5, "month": "2024-06", "release": "RELEASE-2026", "isProvisional": False,
         "isReleaseOnly": False, "fileCount": 1, "totalBytes": 5000, "isExternal": False},
        {"productCode": P_RO, "month": "2019-05", "release": "RELEASE-2024", "isProvisional": False,
         "isReleaseOnly": True, "fileCount": 1, "totalBytes": 100, "isExternal": False},
        {"productCode": P_EXT, "month": "2024-06", "release": "PROVISIONAL", "isProvisional": True,
         "isReleaseOnly": False, "fileCount": 0, "totalBytes": 0, "isExternal": True},
    ]
    f.external_links = [{"productCode": P_EXT, "url": "https://phenocam.nau.edu/webcam/sites/NEON.D99.TEST/",
                         "name": "Phenocam", "type": "Phenocam Website", "monthProbed": "2026-09",
                         "months": ["2024-06"]}]
    fs = {}
    for m in ("2024-06", "2026-08"):
        for hv, qf in (("000.010", 0.48611), ("000.020", 1.25)):
            nm = next(r["name"] for r in files if r["productCode"] == P_IS and r["month"] == m
                      and r["kind"] == "data" and f"{r['hor']}.{r['ver']}" == hv)
            fs[nm] = stats(nm, P_IS, "SAAT_30min", m, 1440, qfFailPct=qf, hor=hv[:3], ver=hv[4:], tmi="030")
    cd = next(r["name"] for r in files if r.get("tableName") == "brd_countdata")
    pp = next(r["name"] for r in files if r.get("tableName") == "brd_perpoint")
    h5 = next(r["name"] for r in files if r.get("tableName") == "nsae")
    ro = next(r["name"] for r in files if r["productCode"] == P_RO)
    plots = [f"{SITE}_{i:03d}" for i in range(60)]
    fs[cd] = stats(cd, P_OS, "brd_countdata", "2024-05", 1178, plotIDs=plots[:5], plotCount=5, taxonCount=2,
                   protocolVersions=["NEON.DOC.014041vL"], timeColumn="startDate")
    fs[pp] = stats(pp, P_OS, "brd_perpoint", "2024-05", 45, plotIDs=plots, plotCount=60,
                   protocolVersions=["NEON.DOC.014041vK", "NEON.DOC.014041vL"], timeColumn="startDate")
    fs[h5] = stats(h5, P_H5, "nsae", "2024-06", 1440, lakeTable="dp4_00200_001__nsae_dp04", qfFailPct=19.2,
                   timeColumn="timeBgn")
    fs[ro] = stats(ro, P_RO, "PRIPRE_30min", "2019-05", 1488, hor="900", ver="000", tmi="030", qfFailPct=0.0)
    f.file_stats = fs
    f.file_taxa = {cd: [{"sourceFile": cd, "productCode": P_OS, "tableName": "brd_countdata", "taxonID": "BTSP",
                         "scientificName": "Amphispiza bilineata", "taxonRank": "species",
                         "vernacularName": "Black-throated Sparrow", "nRecords": 42},
                        {"sourceFile": cd, "productCode": P_OS, "tableName": "brd_countdata", "taxonID": "UNKN",
                         "scientificName": "Unknownus avis", "taxonRank": "species", "vernacularName": None,
                         "nRecords": 1}]}
    sp = {}
    for m in ("2024-06", "2026-08"):
        spf = next(r["name"] for r in files if r["productCode"] == P_IS and r["month"] == m
                   and r["kind"] == "sensor_positions")
        for hv, loc, z in (("000.010", "CFGLOC104458", 0.25), ("000.020", "CFGLOC104464", 1.64)):
            rows = [{"productCode": P_IS, "month": m, "sourceFile": spf, "HOR.VER": hv, "sensorLocationID": loc,
                     "sensorLocationDescription": f"Test SAAT {hv}", "positionStartDateTime":
                         datetime(2010, 1, 1, tzinfo=UTC), "positionEndDateTime": None,
                     "sensorLatitude": 31.910675067553, "sensorLongitude": -110.83542513062736,
                     "sensorElevation": 997.73 + z, "zOffset": z},
                    {"productCode": P_IS, "month": m, "sourceFile": spf, "HOR.VER": hv, "sensorLocationID": "OLDLOC",
                     "sensorLocationDescription": "retired", "positionStartDateTime": datetime(2005, 1, 1, tzinfo=UTC),
                     "positionEndDateTime": datetime(2009, 12, 31, tzinfo=UTC), "sensorLatitude": 1.0,
                     "sensorLongitude": 2.0, "sensorElevation": 3.0, "zOffset": 9.0}]
            sp[(P_IS, m, hv)] = rows
        f.sensor_counts[spf] = 2
    f.sensor_positions = sp
    f.h5_tower = {h5: ("31.910711", "-110.83547")}
    f.lake_tables = {P_IS: ["dp1_00002_001__saat_30min", "sensor_positions", "variables"],
                     P_OS: ["dp1_10003_001__brd_countdata", "dp1_10003_001__brd_perpoint", "validation"],
                     P_H5: ["dp4_00200_001__nsae_dp04"], P_RO: ["dp1_00006_001__pripre_30min"]}
    f.variables = {P_IS: [
        {"productCode": P_IS, "tableName": "SAAT_30min", "fieldName": "tempSingleMean", "units": "celsius",
         "downloadPkg": "basic", "dataType": "real"},
        {"productCode": P_IS, "tableName": "SAAT_30min", "fieldName": "heaterNAQM", "units": "percent",
         "downloadPkg": "expanded", "dataType": "real"},
        {"productCode": P_IS, "tableName": "SAAT_30min", "fieldName": "tempSingleVariance", "units": "celsiusSquared",
         "downloadPkg": "basic", "dataType": "real"},
        {"productCode": P_IS, "tableName": "SAAT_30min", "fieldName": "finalQF", "units": None,
         "downloadPkg": "basic", "dataType": "signed integer"}]}
    f.downloads = {(r["productCode"], r["month"], r["name"]): {
        "dl_status": "ok", "dl_bytes": r["size"] or 50, "dl_md5": r["dlMd5"], "md5_verified": 1,
        "downloaded_at": "2026-09-27T14:59:42.178891+00:00"} for r in files}
    f.ontology = ontology if ontology is not None else make_ontology()
    return f


def write_local_files(facts: Facts, root: Path | None = None) -> None:
    """Create the raw files on disk (variables CSVs with 3 records; others one line)."""
    root = root or env.ROOT
    for r in facts.files:
        p = root / r["localRelPath"]
        p.parent.mkdir(parents=True, exist_ok=True)
        if r["kind"] == "variables":
            p.write_text('table,fieldName,description\nSAAT_30min,a,"x"\nSAAT_30min,b,"multi\nline"\nX,c,z\n')
        elif r["kind"] == "data" and r["ext"] == "csv":
            n = facts.file_stats[r["name"]]["rowCount"] if r["name"] in facts.file_stats else 1
            p.write_text("startDateTime,value\n" + "".join(f"2024-06-01T00:00Z,{i}\n" for i in range(n)))
        else:
            p.write_bytes(f"{r['name']}\n".encode())


def detach_site_avus() -> None:
    """Keep ``neon_ducklake.site`` without an ``avus`` attribute after importing the module.

    tests/test_avu_tag.py simulates a missing site builder with ``sys.modules[...] = None``; that only works
    while the package attribute is unset (``from ..site import avus`` returns an existing attribute first).
    ``from neon_ducklake.site import avus`` keeps working through ``sys.modules``.
    """
    import neon_ducklake.site as pkg

    pkg.__dict__.pop("avus", None)


detach_site_avus()
