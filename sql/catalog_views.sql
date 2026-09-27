-- Views of the catalog lake (ducklake/neon_catalog.ducklake, schema main).
--
-- catalog_lake.py splits this file on the "-- view: <name>: <description>" markers, creates each view inside
-- the build transaction as <alias>.<name>, and records the sha256 of each block in table_catalog so an
-- unchanged view is not re-created (no spurious snapshot change). Table names stay unqualified: DuckDB binds
-- them in the view's own catalog, so the views keep working when the lake is attached under another alias
-- (see sql/attach_all.sql).

-- view: v_site_inventory: Phase-2 planner: one row per non-AOP product x site x month (current + release-only) with file totals
CREATE OR REPLACE VIEW v_site_inventory AS
WITH f AS (
    SELECT productCode, siteCode, month, count(*) AS fileCount, sum(coalesce(size, 0)) AS totalBytes
    FROM files
    GROUP BY productCode, siteCode, month
), ext AS (
    SELECT DISTINCT productCode, siteCode FROM external_data
)
SELECT a.productCode, a.siteCode, a.month,
       p.productName, p.productScienceTeamAbbr,
       a.release, a.isProvisional, a.isReleaseOnly,
       ext.productCode IS NOT NULL AS isExternal,
       coalesce(f.fileCount, 0)::BIGINT AS fileCount,
       coalesce(f.totalBytes, 0)::BIGINT AS totalBytes
FROM availability_months a
LEFT JOIN products p ON p.productCode = a.productCode
LEFT JOIN f ON f.productCode = a.productCode AND f.siteCode = a.siteCode AND f.month = a.month
LEFT JOIN ext ON ext.productCode = a.productCode AND ext.siteCode = a.siteCode
WHERE NOT coalesce(p.isAop, regexp_matches(a.productCode, '^DP[1-4]\.30\d{3}\.\d{3}$'));

-- view: v_pair_coverage: per non-AOP product x site: cells with files, external, or missing, with the inventory status
CREATE OR REPLACE VIEW v_pair_coverage AS
SELECT i.productCode, i.siteCode,
       any_value(ps.inventoryScope) AS inventoryScope,
       any_value(ps.inventoryStatus) AS inventoryStatus,
       count(*) AS cells,
       count(*) FILTER (WHERE i.fileCount > 0) AS cellsWithFiles,
       count(*) FILTER (WHERE i.fileCount = 0 AND i.isExternal) AS cellsExternal,
       count(*) FILTER (WHERE i.fileCount = 0 AND NOT i.isExternal) AS cellsMissing,
       count(*) FILTER (WHERE i.isProvisional) AS provisionalCells,
       count(*) FILTER (WHERE i.isReleaseOnly) AS releaseOnlyCells,
       sum(i.fileCount)::BIGINT AS fileCount,
       sum(i.totalBytes)::BIGINT AS totalBytes,
       min(i.month) AS firstMonth,
       max(i.month) AS lastMonth
FROM v_site_inventory i
LEFT JOIN product_site ps ON ps.productCode = i.productCode AND ps.siteCode = i.siteCode
GROUP BY i.productCode, i.siteCode;

-- view: v_product_summary: per product: status, team, DOI, pairs, cells (provisional / release-only) and inventory totals
CREATE OR REPLACE VIEW v_product_summary AS
WITH ps AS (
    SELECT productCode,
           count(*) AS pairCount,
           count(*) FILTER (WHERE inventoryScope = 'release-only') AS releaseOnlyPairs,
           count(*) FILTER (WHERE inventoryStatus = 'ok') AS pairsWithFiles,
           count(*) FILTER (WHERE inventoryStatus = 'external') AS externalPairs,
           count(*) FILTER (WHERE inventoryStatus IN ('error', 'missing')) AS failedPairs,
           sum(fileCount)::BIGINT AS fileCount,
           sum(totalBytes)::BIGINT AS totalBytes
    FROM product_site
    GROUP BY productCode
), am AS (
    SELECT productCode,
           count(*) AS cells,
           count(*) FILTER (WHERE isProvisional) AS provisionalCells,
           count(*) FILTER (WHERE isReleaseOnly) AS releaseOnlyCells,
           min(month) AS firstMonth,
           max(month) AS lastMonth
    FROM availability_months
    GROUP BY productCode
)
SELECT p.productCode, p.productName, p.productScienceTeamAbbr, p.productStatus, p.isAop,
       p.latestRelease, p.latestReleaseDoi,
       coalesce(ps.pairCount, 0) AS pairCount,
       coalesce(ps.releaseOnlyPairs, 0) AS releaseOnlyPairs,
       coalesce(ps.pairsWithFiles, 0) AS pairsWithFiles,
       coalesce(ps.externalPairs, 0) AS externalPairs,
       coalesce(ps.failedPairs, 0) AS failedPairs,
       coalesce(am.cells, 0) AS cells,
       coalesce(am.provisionalCells, 0) AS provisionalCells,
       coalesce(am.releaseOnlyCells, 0) AS releaseOnlyCells,
       am.firstMonth, am.lastMonth,
       ps.fileCount, ps.totalBytes
FROM products p
LEFT JOIN ps ON ps.productCode = p.productCode
LEFT JOIN am ON am.productCode = p.productCode;
