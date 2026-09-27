"""Tiny hermetic harvest run (raw landings, ledger, stage files) for the catalog tests.

Shape mirrors the real NEON payloads harvested on 2026-09-27, cut down to a handful of rows:
- DP1.00001.001 (TIS) current at SRER (2020-01, 2020-02 in RELEASE-2025, 2020-03 PROVISIONAL) and HARV
  (2020-01, zero files, external link);
- DP1.10003.001 (TOS) current at HARV 2020-06 with DP0 support files;
- DP1.00006.001 (TIS) release-only at SRER (2020-01 newest in RELEASE-2025, 2020-02 only in RELEASE-2024);
- DP3.30006.001 (AOP) current at SRER 2020-07 plus a release-only AOP cell (excluded from the inventory).
"""

from __future__ import annotations

import copy
import gzip
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

RUN_ID = "20260101T000000Z"
BASE = "https://data.neonscience.org/api/v0"
GCS = "https://storage.googleapis.com/neon-publication"


def _release(tag: str, gen: str, doi: str) -> dict[str, Any]:
    return {"release": tag, "generationDate": gen, "url": f"{BASE}/releases/{tag}",
            "productDoi": {"generationDate": gen, "url": f"https://doi.org/10.48443/{doi}"}}


def _site_code(code: str, rels: dict[str, list[str]], product: str) -> dict[str, Any]:
    months = sorted(m for ms in rels.values() for m in ms)
    return {"siteCode": code, "availableMonths": months,
            "availableDataUrls": [f"{BASE}/data/{product}/{code}/{m}" for m in months],
            "availableReleases": [{"release": r, "availableMonths": ms} for r, ms in rels.items()]}


def product(code: str, name: str, team: str, sites: dict[str, dict[str, list[str]]], *, status: str = "ACTIVE",
            releases: list[dict[str, Any]] | None = None, bio: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "productCodeLong": f"NEON.DOM.SITE.{code}", "productCode": code, "productCodePresentation": f"NEON.{code[:9]}",
        "productName": name, "productDescription": f"{name} description", "productStatus": status,
        "productCategory": "Level 1 Data Product", "productHasExpanded": True, "productScienceTeamAbbr": team,
        "productScienceTeam": f"Team {team}", "productPublicationFormatType": f"{team} Data Product Type",
        "productAbstract": "abstract", "productDesignDescription": "design", "productStudyDescription": "study",
        "productBasicDescription": "basic", "productExpandedDescription": "expanded", "productSensor": None,
        "productRemarks": None, "themes": ["Atmosphere"], "keywords": ["wind", "tower"],
        "changeLogs": [{"id": int(hashlib.md5(code.encode()).hexdigest()[:6], 16), "parentIssueID": None,
                        "issueDate": "2018-01-05T00:00:00Z", "resolvedDate": "2018-01-05T00:00:00Z",
                        "dateRangeStart": "2017-12-28T00:00:00Z", "dateRangeEnd": None,
                        "locationAffected": "SRER", "issue": "ice", "resolution": "flagged"}],
        "specs": [{"specId": "8365", "specNumber": "NEON.DOC.000780vD", "specType": "application/pdf",
                   "specSize": 715529, "specDescription": "ATBD", "specUrl": f"{BASE}/documents/NEON.DOC.000780vD"},
                  {"specId": "9", "specNumber": f"NEON.QSG.{code}v1", "specType": "application/pdf",
                   "specSize": 1000, "specDescription": "QSG", "specUrl": f"{BASE}/documents/NEON.QSG.{code}v1"}],
        "biorepositoryCollections": bio,
        "releases": releases if releases is not None else [_release("RELEASE-2025", "2025-01-29T00:07:30Z",
                                                                    "abcd-" + code[4:9])],
        "siteCodes": [_site_code(s, rels, code) for s, rels in sites.items()] or None,
    }


def fname(domain: str, site: str, code: str, rest: str) -> str:
    return f"NEON.{domain}.{site}.{code}.{rest}"


def dq_file(name: str, size: int | None, site: str, product_code: str, month: str) -> dict[str, Any]:
    return {"name": name, "size": size, "md5": hashlib.md5(name.encode()).hexdigest(), "crc32c": None,
            "url": f"{GCS}/NEON.DOM.SITE.{product_code}/{site}/{month}/basic/{name}"}


def package(domain: str, site: str, month: str, gen: str, files: list[dict[str, Any]]) -> dict[str, Any]:
    return {"domainCode": domain, "siteCode": site, "month": month, "packageType": "basic",
            "generationDate": gen, "files": files}


def dq_payload(code: str, site: str, window: tuple[str, str], releases: list[dict[str, Any]],
               release: str | None) -> dict[str, Any]:
    return {"productCode": code, "siteCodes": [site], "startDate": f"{window[0]}-01T00:00:00Z",
            "endDate": f"{window[1]}-28T23:59:59Z", "packageType": "basic", "releases": releases,
            "_request": {"pair": {"product": code, "site": site, "release": release,
                                  "scope": "release-only" if release else "current"},
                         "window": list(window)}}


def is_files(domain: str, site: str, code: str, month: str, ts: str) -> list[dict[str, Any]]:
    names = [fname(domain, site, code, f"000.010.030.2DWSD_30min.{month}.basic.{ts}.csv"),
             fname(domain, site, code, f"000.010.002.2DWSD_2min.{month}.basic.{ts}.csv"),
             fname(domain, site, code, f"readme.{ts}.txt"),
             fname(domain, site, code, f"sensor_positions.{ts}.csv"),
             fname(domain, site, code, f"EML.{month.replace('-', '')}01-{month.replace('-', '')}28.{ts}.xml")]
    return [dq_file(n, None if ".EML." in n or ".readme." in n else 1000 + i, site, code, month)
            for i, n in enumerate(names)]


def build_payloads() -> dict[str, dict[str, Any]]:
    """family -> key -> payload (the raw landings of the fake run)."""
    p1 = product("DP1.00001.001", "2D wind speed and direction", "TIS",
                 {"SRER": {"RELEASE-2025": ["2020-01", "2020-02"], "PROVISIONAL": ["2020-03"]},
                  "HARV": {"RELEASE-2025": ["2020-01"]}},
                 releases=[_release("RELEASE-2024", "2024-01-26T00:00:00Z", "old1-0001"),
                           _release("RELEASE-2025", "2025-01-29T00:07:30Z", "s9ya-zc81")])
    p2 = product("DP1.10003.001", "Breeding landbird point counts", "TOS", {"HARV": {"RELEASE-2025": ["2020-06"]}},
                 bio=[{"collectionCode": "BRD", "collectionName": "Bird Collection",
                       "collectionUrl": "https://biorepo.neonscience.org/c/1", "collectionContentUrl": None,
                       "collectionDownloadUrl": None}])
    p3 = product("DP1.00006.001", "Precipitation", "TIS", {},
                 releases=[_release("RELEASE-2024", "2024-01-26T00:00:00Z", "3cp7-7s80"),
                           _release("RELEASE-2025", "2025-01-29T00:07:30Z", "3cp7-7s81")])
    p4 = product("DP3.30006.001", "Spectrometer orthorectified surface directional reflectance", "AOP",
                 {"SRER": {"RELEASE-2025": ["2020-07"]}})
    products = [p1, p2, p3, p4]
    manifests = {
        "RELEASE-2024": {"uuid": "u-2024", "dpSiteMonths": [
            {"dp": "NEON.DOM.SITE.DP1.00001.001", "siteMonths": [{"site": "SRER", "months": ["2020-01"]}]},
            {"dp": "NEON.DOM.SITE.DP1.00006.001", "siteMonths": [{"site": "SRER", "months": ["2020-01", "2020-02"]}]},
            {"dp": "NEON.DOM.SITE.DP3.30006.001", "siteMonths": [{"site": "SRER", "months": ["2019-07"]}]}],
            "_artifact": {"name": "manifest-available-2024.json", "md5": "m24", "size": 10, "md5Verified": True}},
        "RELEASE-2025": {"uuid": "u-2025", "dpSiteMonths": [
            {"dp": "NEON.DOM.SITE.DP1.00001.001", "siteMonths": [{"site": "SRER", "months": ["2020-01", "2020-02"]},
                                                                 {"site": "HARV", "months": ["2020-01"]}]},
            {"dp": "NEON.DOM.SITE.DP1.00006.001", "siteMonths": [{"site": "SRER", "months": ["2020-01"]}]},
            {"dp": "NEON.DOM.SITE.DP1.10003.001", "siteMonths": [{"site": "HARV", "months": ["2020-06", "2020-06"]}]},
            {"dp": "NEON.DOM.SITE.DP3.30006.001", "siteMonths": [{"site": "SRER", "months": ["2020-07"]}]}],
            "_artifact": {"name": "manifest-available-2025.json", "md5": "m25", "size": 10, "md5Verified": True}},
    }
    releases = [
        {"release": tag, "uuid": f"u-{tag[-4:]}", "generationDate": f"{tag[-4:]}-01-26T00:00:00Z",
         "artifacts": [{"name": manifests[tag]["_artifact"]["name"], "type": "MANIFEST_AVAILABLE",
                        "url": f"{GCS}/release/build/u/{manifests[tag]['_artifact']['name']}", "size": 10,
                        "md5": manifests[tag]["_artifact"]["md5"]}],
         "dataProducts": [{"productCode": p["productCode"], "productName": p["productName"],
                           "productDescription": "d",
                           "productDoi": next(r["productDoi"]["url"] for r in p["releases"] if r["release"] == tag)}
                          for p in products if any(r["release"] == tag for r in p["releases"])]}
        for tag in ("RELEASE-2024", "RELEASE-2025")]
    release_products = {}
    release_sites = {}
    for tag in ("RELEASE-2024", "RELEASE-2025"):
        recs = []
        for p in products:
            rp = copy.deepcopy(p)
            rp["releases"] = [r for r in p["releases"] if r["release"] == tag]
            cells: dict[str, list[str]] = {}
            for dp in manifests[tag]["dpSiteMonths"]:
                if dp["dp"].endswith(p["productCode"]):
                    for sm in dp["siteMonths"]:
                        cells[sm["site"]] = sorted(set(sm["months"]))
            rp["siteCodes"] = [_site_code(s, {tag: ms}, p["productCode"]) for s, ms in cells.items()]
            recs.append(rp)
        release_products[tag] = recs
        release_sites[tag] = [
            {"siteCode": s, "siteName": s, "releases": [{"release": tag}],
             "dataProducts": [{"dataProductCode": r["productCode"], "dataProductTitle": r["productName"],
                               "availableMonths": sc["availableMonths"], "availableDataUrls": [],
                               "availableReleases": [{"release": tag, "availableMonths": sc["availableMonths"]}]}
                              for r in recs for sc in r["siteCodes"] if sc["siteCode"] == s]}
            for s in ("SRER", "HARV")]
    sites = []
    for code, dom, dname, lat in (("SRER", "D14", "Desert Southwest", 31.9), ("HARV", "D01", "Northeast", 42.5)):
        dps = [{"dataProductCode": p["productCode"], "dataProductTitle": p["productName"],
                "availableMonths": sc["availableMonths"], "availableDataUrls": sc["availableDataUrls"],
                "availableReleases": sc["availableReleases"]}
               for p in products for sc in p["siteCodes"] or [] if sc["siteCode"] == code]
        sites.append({"siteCode": code, "siteName": f"{code} NEON", "siteDescription": f"{code} site",
                      "siteType": "CORE", "siteLatitude": lat, "siteLongitude": -110.8, "stateCode": "AZ",
                      "stateName": "Arizona", "domainCode": dom, "domainName": dname,
                      "deimsId": f"https://deims.org/{code}",
                      "releases": [{"release": "RELEASE-2025", "generationDate": "2025-01-29T00:07:30Z",
                                    "url": f"{BASE}/releases/RELEASE-2025"}], "dataProducts": dps})

    def loc(name: str, typ: str, site: str, dom: str, **kw: Any) -> dict[str, Any]:
        base = {"locationName": name, "locationType": typ, "locationDescription": f"{name} desc", "siteCode": site,
                "domainCode": dom, "locationDecimalLatitude": 31.9, "locationDecimalLongitude": -110.8,
                "locationElevation": 997.5, "locationUtmEasting": 515000.0, "locationUtmNorthing": 3530000.0,
                "locationUtmZone": 12, "locationUtmHemisphere": "N", "alphaOrientation": 0.0,
                "betaOrientation": 0.0, "gammaOrientation": 0.0, "xOffset": 0.0, "yOffset": 0.0, "zOffset": 0.0,
                "offsetLocation": None, "locationParent": dom,
                "activePeriods": [{"activatedDate": None, "deactivatedDate": None}],
                "locationProperties": [{"locationPropertyName": "Value for TypeEco",
                                        "locationPropertyValue": "Desert shrubland"},
                                       {"locationPropertyName": "Value for UTM Zone",
                                        "locationPropertyValue": 12.0}],
                "locationPolygon": None}
        base.update(kw)
        return base

    site_locs = [loc("SRER", "SITE", "SRER", "D14", locationChildren=["TOWER1"]),
                 loc("HARV", "SITE", "HARV", "D01", locationElevation=348.1, locationUtmZone=18),
                 loc("MD01", "SITE", "MD01", "D01")]
    for s in site_locs:
        s.pop("locationPolygon")
    tower = loc("TOWER1", "TOWER", "SRER", "D14", locationParent="SRER",
                offsetLocation={"locationName": "SRER"})
    plot = loc("SRER_001.basePlot.all", "OS Plot - all", "SRER", "D14", locationParent="SRER",
               locationPolygon={"coordinates": [{"latitude": 31.0, "longitude": -110.0, "elevation": 1.0},
                                                {"latitude": 31.0, "longitude": -110.1, "elevation": 1.0},
                                                {"latitude": 31.1, "longitude": -110.1, "elevation": 1.0}]})
    batch = {"names": ["D14", "SRER", "TOWER1", "SRER_001.basePlot.all", "GHOST"],
             "rows": [loc("D14", "DOMAIN", None, "D14", locationParent="REALM"),
                      {k: v for k, v in site_locs[0].items() if k != "locationChildren"} | {"locationPolygon": None},
                      tower, plot], "errors": None}
    nodes = {"REALM": {"hierarchyDepth": 0, "hierarchyParent": None, "hierarchySite": None, "locationType": "REALM"},
             "D14": {"hierarchyDepth": 0, "hierarchyParent": "REALM", "hierarchySite": None, "locationType": "DOMAIN"},
             "SRER": {"hierarchyDepth": 0, "hierarchyParent": "D14", "hierarchySite": "SRER", "locationType": "SITE"},
             "TOWER1": {"hierarchyDepth": 1, "hierarchyParent": "SRER", "hierarchySite": "SRER",
                        "locationType": "TOWER"},
             "SRER_001.basePlot.all": {"hierarchyDepth": 1, "hierarchyParent": "SRER", "hierarchySite": "SRER",
                                       "locationType": "OS Plot - all"},
             "GHOST": {"hierarchyDepth": 2, "hierarchyParent": "TOWER1", "hierarchySite": "SRER",
                       "locationType": "CONFIG"}}
    history = {**tower, "locationHistory": [
        {"current": False, "locationStartDate": "2010-01-01T00:00:00Z", "locationEndDate": "2019-06-01T00:00:00Z",
         "alphaOrientation": 0.0, "betaOrientation": 0.0, "gammaOrientation": 0.0, "xOffset": 0.0, "yOffset": 0.0,
         "zOffset": 8.0, "offsetLocation": None, "locationProperties": []},
        {"current": True, "locationStartDate": "2019-06-01T00:00:00Z", "locationEndDate": None,
         "locationDecimalLatitude": 31.9, "locationDecimalLongitude": -110.8, "locationElevation": 1000.0,
         "alphaOrientation": 0.0, "betaOrientation": 0.0, "gammaOrientation": 0.0, "xOffset": 0.0, "yOffset": 0.0,
         "zOffset": 8.5, "offsetLocation": {"locationName": "SRER"}, "locationProperties": [
             {"locationPropertyName": "Value for Height", "locationPropertyValue": "8.5"}]}]}
    taxa = {"count": 2, "total": 2, "prev": None, "next": None, "data": [
        {"taxonTypeCode": "BIRD", "taxonID": "AMRO", "acceptedTaxonID": "AMRO", "updateDate": "2022-04-05T14:10:34Z",
         "dwc:scientificName": "Turdus migratorius", "dwc:scientificNameAuthorship": "Linnaeus, 1766",
         "dwc:taxonRank": "species", "dwc:vernacularName": "American Robin", "taxonProtocolCategory": None,
         "dwc:nameAccordingToID": "doi: 10.1642/AUK-14-124.1", "dwc:nameAccordingToTitle": "AOU",
         "dwc:kingdom": "Animalia", "gbif:subkingdom": None, "dwc:phylum": "Chordata", "dwc:class": "Aves",
         "dwc:order": "Passeriformes", "dwc:family": "Turdidae", "dwc:genus": "Turdus", "dwc:subgenus": None,
         "speciesGroup": None, "dwc:specificEpithet": "migratorius", "dwc:infraspecificEpithet": None},
        {"taxonTypeCode": "BIRD", "taxonID": "BLJA", "acceptedTaxonID": "BLJA", "updateDate": None,
         "dwc:scientificName": "Cyanocitta cristata", "dwc:scientificNameAuthorship": None,
         "dwc:taxonRank": "species", "dwc:vernacularName": "Blue Jay", "taxonProtocolCategory": None,
         "dwc:nameAccordingToID": None, "dwc:nameAccordingToTitle": None, "dwc:kingdom": "Animalia",
         "gbif:subkingdom": None, "dwc:phylum": "Chordata", "dwc:class": "Aves", "dwc:order": "Passeriformes",
         "dwc:family": "Corvidae", "dwc:genus": "Cyanocitta", "dwc:subgenus": None, "speciesGroup": None,
         "dwc:specificEpithet": "cristata", "dwc:infraspecificEpithet": None}]}
    uuid = "07dccab1-4990-4d2f-9c5b-bf97e10517e3"
    proto_file = {"name": "Dataset EML", "description": "Generated dataset EML", "size": 15071,
                  "fileName": f"NEON.PROTOTYPE.DATASET.EML.{uuid}.xml", "md5": "674c",
                  "type": {"name": "EML", "description": "EML XML metadata file"},
                  "url": f"https://neon-publication-proto.storage.googleapis.com/datasets/{uuid}/x.xml"}
    proto = [{"uuid": uuid, "projectTitle": "Reaeration BLUE 2016", "projectDescription": "p",
              "designDescription": "d", "metadataDescription": "m", "studyAreaDescription": "s",
              "datasetAbstract": "a", "startYear": 2016, "endYear": 2016, "dateUploaded": "2021-02-04T22:38:29Z",
              "isPublished": False, "version": "v1", "versionDescription": None,
              "doi": {"url": "https://doi.org/10.48443/7ay8-e395", "generationDate": "2021-05-12T23:18:08Z"},
              "relatedVersions": [], "data": {"url": f"{BASE}/prototype/data/{uuid}", "files": [proto_file],
                                             "dataLocations": []},
              "dataThemes": ["Ecohydrology"], "fileTypes": [{"name": "CSV", "description": "CSV"}],
              "keywords": ["tracer"], "locations": [{"domain": "D08", "state": "OK", "siteCode": "BLUE",
                                                     "siteName": "Blue River", "latitude": 34.4, "longitude": -96.6}],
              "publicationCitations": [], "relatedDataProducts": [
                  {"dataProductIdq": "NEON.DOM.SITE.DP1.20190.001", "dataProductCode": "DP1.20190.001"}],
              "scienceTeams": ["AOS"]}]
    gql = {"data": {"__schema": {"queryType": {"name": "Query"}, "mutationType": None, "subscriptionType": None,
                                 "types": [
        {"kind": "OBJECT", "name": "Query", "description": None, "inputFields": None, "interfaces": [],
         "enumValues": None, "possibleTypes": None, "fields": [
             {"name": "findLocations", "description": "find", "isDeprecated": False, "deprecationReason": None,
              "args": [{"name": "query", "description": None, "defaultValue": None,
                        "type": {"kind": "NON_NULL", "name": None,
                                 "ofType": {"kind": "INPUT_OBJECT", "name": "LocationQuery", "ofType": None}}}],
              "type": {"kind": "NON_NULL", "name": None, "ofType": {"kind": "LIST", "name": None, "ofType": {
                  "kind": "OBJECT", "name": "Location", "ofType": None}}}}]},
        {"kind": "ENUM", "name": "Kind", "description": "k", "fields": None, "inputFields": None, "interfaces": None,
         "possibleTypes": None, "enumValues": [{"name": "A", "description": None, "isDeprecated": False,
                                                "deprecationReason": None}]}]}}, "errors": None}

    # file inventory
    srer_cur = dq_payload("DP1.00001.001", "SRER", ("2020-01", "2020-03"), [
        {"release": "RELEASE-2025", "generationDate": "2025-01-29T00:07:30Z", "packages": [
            package("D14", "SRER", m, "2025-01-29T00:07:30Z", is_files("D14", "SRER", "DP1.00001.001", m,
                                                                     "20241210T212549Z"))
            for m in ("2020-01", "2020-02")]},
        {"release": "PROVISIONAL", "generationDate": None, "packages": [
            package("D14", "SRER", "2020-03", "2026-04-01T00:00:00Z",
                    is_files("D14", "SRER", "DP1.00001.001", "2020-03", "20260401T000000Z"))]}], None)
    harv_zero = dq_payload("DP1.00001.001", "HARV", ("2020-01", "2020-01"), [], None)
    brd = [dq_file(fname("D01", "HARV", "DP1.10003.001", f"brd_countdata.2020-06.basic.{ts}.csv"), 500,
                   "HARV", "DP1.10003.001", "2020-06") for ts in ("20250106T223127Z",)]
    brd += [dq_file(fname("D01", "HARV", "DP0.10003.001", "categoricalCodes.20250106T223127Z.csv"), 100,
                    "HARV", "DP1.10003.001", "2020-06"),
            dq_file(fname("D01", "HARV", "DP0.10003.001", "validation.20250106T223127Z.csv"), 90,
                    "HARV", "DP1.10003.001", "2020-06"),
            dq_file(fname("D01", "HARV", "DP1.10003.001", "variables.20250106T223127Z.csv"), 80,
                    "HARV", "DP1.10003.001", "2020-06")]
    brd_q = dq_payload("DP1.10003.001", "HARV", ("2020-06", "2020-06"), [
        {"release": "RELEASE-2025", "generationDate": "2025-01-29T00:07:30Z",
         "packages": [package("D01", "HARV", "2020-06", "2025-01-29T00:07:30Z", brd)]}], None)
    ro25 = dq_payload("DP1.00006.001", "SRER", ("2020-01", "2020-01"), [
        {"release": "RELEASE-2025", "generationDate": "2025-01-29T00:07:30Z", "packages": [
            package("D14", "SRER", "2020-01", "2025-01-29T00:07:30Z",
                    is_files("D14", "SRER", "DP1.00006.001", "2020-01", "20241210T000000Z")[:3])]}], "RELEASE-2025")
    ro24 = dq_payload("DP1.00006.001", "SRER", ("2020-02", "2020-02"), [
        {"release": "RELEASE-2024", "generationDate": "2024-01-26T00:00:00Z", "packages": [
            package("D14", "SRER", "2020-02", "2024-01-26T00:00:00Z",
                    is_files("D14", "SRER", "DP1.00006.001", "2020-02", "20231210T000000Z")[:2])]}], "RELEASE-2024")
    external = {"productCode": "DP1.00001.001", "siteCode": "HARV", "month": "2020-01", "release": "RELEASE-2025",
                "packages": [], "files": [], "externalData": [
                    {"name": "Phenocam", "type": "Phenocam Website",
                     "url": "https://phenocam.nau.edu/webcam/sites/NEON.D01.HARV.DP1.00033/"}]}
    specs = sorted({s["specNumber"] for p in products for s in p["specs"]})
    heads = {s: ({"specNumber": s, "status": 200, "contentType": "application/pdf", "contentLength": 10,
                  "fileName": f"{s}.pdf"} if "QSG" not in s else
                 {"specNumber": s, "status": 404, "error": f"NeonApiError: 404 /documents/{s}: no detail"})
             for s in specs}
    return {
        "products": {"products": products}, "sites": {"sites": sites}, "releases": {"releases": releases},
        "manifests": manifests, "release_products": release_products, "release_sites": release_sites,
        "locations_sites": {"locations_sites": site_locs},
        "samples": {"supportedClasses": {"entries": [{"key": "wdp_collection_in.sampleID", "value": "Wet dep"},
                                                     {"key": "brd.sampleID", "value": "Bird"}]}},
        "prototype": {"datasets": proto},
        "prototype_data": {uuid: {"datasetUuid": uuid, "datasetProjectTitle": "Reaeration BLUE 2016",
                                  "files": [proto_file], "dataLocations": []}},
        "graphql": {"schema": gql}, "taxonomy": {"BIRD__0": taxa}, "documents_head": heads,
        "location_hierarchy": {"SRER": {"locationName": "SRER", "locationChildren": []}},
        "locations": {"batch_00000": batch}, "location_history": {"TOWER1": history},
        "data_query": {"DP1.00001.001__SRER__2020-01_2020-03": srer_cur,
                       "DP1.00001.001__HARV__2020-01_2020-01": harv_zero,
                       "DP1.10003.001__HARV__2020-06_2020-06": brd_q,
                       "DP1.00006.001__SRER__2020-01_2020-01__RELEASE-2025": ro25,
                       "DP1.00006.001__SRER__2020-02_2020-02__RELEASE-2024": ro24},
        "external": {"DP1.00001.001__HARV__2020-01": external},
        "_stage": {"location_nodes": nodes},
    }


@dataclass
class FakeRun:
    root: Path
    run_dir: Path
    ledger_db: Path
    payloads: dict[str, dict[str, Any]]

    @property
    def run_id(self) -> str:
        return self.run_dir.name

    def land(self, family: str, key: str, payload: Any, *, fetched_at: str = "2026-01-01T00:00:00+00:00",
             status: str = "ok") -> Path:
        """Write one landing + its ledger row, exactly like harvest.Harvest.land."""
        from neon_ducklake import state

        safe = re.sub(r"[^A-Za-z0-9._=+-]", "_", key)
        p = self.run_dir / "raw" / family / f"{safe}.json.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        blob = gzip.compress(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode())
        p.write_bytes(blob)
        con = state.connect(self.ledger_db)
        method = "POST" if family in ("data_query", "graphql", "locations") else "GET"
        con.execute("INSERT OR REPLACE INTO requests(run_id,family,key,method,path,params,http_status,attempts,"
                    "elapsed_ms,response_bytes,landed_path,landed_bytes,sha256,status,error,fetched_at) VALUES "
                    "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (self.run_id, family, key, method, f"/{family}", json.dumps({"k": key}), 200, 1, 1.0, None,
                     str(p.relative_to(self.run_dir)), len(blob), hashlib.sha256(blob).hexdigest(), status, None,
                     fetched_at))
        con.close()
        return p

    def ledger_row(self, family: str, key: str, status: str, error: str | None = None) -> None:
        from neon_ducklake import state

        con = state.connect(self.ledger_db)
        con.execute("INSERT OR REPLACE INTO requests(run_id,family,key,method,path,params,status,error,fetched_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?)", (self.run_id, family, key, "POST", "/data/query", "{}", status,
                                                    error, "2026-01-01T00:00:01+00:00"))
        con.close()

    def write_stage(self, name: str, obj: Any) -> None:
        p = self.run_dir / "stage" / f"{name}.json.gz"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(gzip.compress(json.dumps(obj).encode()))

    def set_status(self, status: str) -> None:
        (self.run_dir / "fetch_manifest.json").write_text(json.dumps({
            "runId": self.run_id, "status": status, "stages": ["catalog", "inventory", "locations"], "pilot": False,
            "startedAt": "2026-01-01T00:00:00+00:00", "finishedAt": "2026-01-01T01:00:00+00:00", "seconds": 3600.0,
            "pairs": 5, "failureCount": 0, "versions": {"neon-mcp": "abc"}, "tools": {"duckdb": "1.5.5"},
            "tunables": {}, "counts": {}, "transforms": ["signed-url query strings stripped"]}))


def make_run(root: Path, payloads: dict[str, dict[str, Any]] | None = None, status: str = "complete") -> FakeRun:
    from neon_ducklake.harvest import Harvest

    payloads = payloads or build_payloads()
    run_dir = root / "api" / "runs" / RUN_ID
    run = FakeRun(root, run_dir, root / "work" / "state.sqlite", payloads)
    for family, items in payloads.items():
        if family.startswith("_"):
            continue
        for key, payload in items.items():
            run.land(family, key, payload)
    run.write_stage("location_nodes", payloads["_stage"]["location_nodes"])
    plan = Harvest.plan_pairs(payloads["products"]["products"], payloads["manifests"])
    run.write_stage("pairs", plan)
    run.write_stage("zero_pairs", [p for p in plan if p["product"] == "DP1.00001.001" and p["site"] == "HARV"])
    run.set_status(status)
    return run


def make_precedent(root: Path) -> tuple[Path, Path, int]:
    """A tiny neon-downloads-like spatial lake: layer_catalog + one neon_* GEOMETRY layer; returns (lake, data, v)."""
    import duckdb

    d = root / "precedent"
    d.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("LOAD ducklake; LOAD spatial")
    con.execute(f"ATTACH 'ducklake:{d / 'neon_spatial.ducklake'}' AS src (DATA_PATH '{d / 'data'}/')")
    con.execute("CREATE TABLE src.layer_catalog (table_name VARCHAR, kind VARCHAR, geometry VARCHAR, rows BIGINT, "
                "source_file VARCHAR, source_url VARCHAR, source_layer VARCHAR, loaded_at TIMESTAMP)")
    con.execute("CREATE TABLE src.neon_domains AS SELECT 'D14' AS domainID, ST_Point(-110.8, 31.9) AS geometry")
    con.execute("INSERT INTO src.layer_catalog VALUES ('neon_domains', 'neonscience.org spatial layer', 'Point', 1, "
                "'NEONDomains_2024.zip', 'https://www.neonscience.org/x.zip', NULL, now()), "
                "('api_sites', 'NEON Data API', 'Point', 81, 'sites.json', NULL, NULL, now())")
    version = con.execute("SELECT max(snapshot_id) FROM src.snapshots()").fetchone()[0]
    con.execute("INSERT INTO src.neon_domains SELECT 'D01', ST_Point(-72.1, 42.5)")  # after the pinned version
    con.execute("DETACH src")
    con.close()
    return d / "neon_spatial.ducklake", d / "data", version


@pytest.fixture
def fake_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point every env path at tmp_path and stub the slow provenance helpers."""
    from neon_ducklake import env, verify_catalog

    root = tmp_path / "root"
    root.mkdir()
    monkeypatch.setattr(env, "ROOT", root)
    monkeypatch.setattr(env, "API_DIR", root / "api")
    monkeypatch.setattr(env, "RUNS_DIR", root / "api" / "runs")
    monkeypatch.setattr(env, "LAKE_DIR", root / "ducklake")
    monkeypatch.setattr(env, "WORK_DIR", root / "work")
    monkeypatch.setattr(env, "LOCK_DIR", root / "work" / "locks")
    monkeypatch.setattr(env, "DUCKDB_SETTINGS", {"memory_limit": "2GB", "threads": 2})
    monkeypatch.setattr(env, "repo_versions", lambda: {"neon-ducklake": "test"})
    monkeypatch.setattr(env, "tool_versions", lambda: {"duckdb": "test"})
    monkeypatch.setattr(verify_catalog, "EXCEPTIONS_FILE", root / "ref" / "catalog_coverage_exceptions.json")
    return root


def test_fake_run_lands_every_family(tmp_path: Path) -> None:
    run = make_run(tmp_path)
    fams = sorted(p.name for p in (run.run_dir / "raw").iterdir())
    assert "data_query" in fams and "manifests" in fams and "taxonomy" in fams
    assert (run.run_dir / "stage" / "pairs.json.gz").exists()
    assert json.loads((run.run_dir / "fetch_manifest.json").read_text())["status"] == "complete"
