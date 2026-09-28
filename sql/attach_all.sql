-- Attach every neon-ducklake lake READ_ONLY. Run it from the repository (data) root: every path below is
-- relative to the working directory.
--
-- Usage (DuckDB 1.5.5 CLI or Python, extensions already installed, this file only LOADs them):
--   cd <neon-ducklake root> && duckdb -init sql/attach_all.sql
--   D .read sql/attach_all.sql
--   python: neon_ducklake.catalog_lake.attach_all(con), which attaches the same way but with absolute paths
--           from any working directory, and only the lakes that exist (it globs sites/*/ducklake/neon_*.ducklake).
--           Executing this whole file from Python stops at the first site lake that does not exist yet.
--
-- Every ATTACH gives an explicit DATA_PATH plus OVERRIDE_DATA_PATH true, so the 'data/' path the writers
-- record is replaced by the one given here. READ_ONLY never takes a writer lock, so this is safe while nothing
-- else is building. A lake that is being written (catalog build, site stack) may refuse the attach until the
-- writer detaches.
--
-- Aliases: neon (catalog), neon_<site> (site lakes, lower-case site code). Views keep unqualified table
-- names, so e.g. `SELECT * FROM neon.v_site_inventory WHERE siteCode = 'SRER'` works under any alias.
-- A site lake that does not exist yet fails its ATTACH: the DuckDB CLI reports the error and carries on
-- (use `.bail off`, the default). Add further sites by copying one block.

LOAD ducklake;
LOAD spatial;

-- Phase-1 catalog lake
ATTACH IF NOT EXISTS 'ducklake:ducklake/neon_catalog.ducklake' AS neon
    (READ_ONLY, DATA_PATH 'ducklake/data/', OVERRIDE_DATA_PATH true);

-- Phase-2 site lakes (sites/<SITE>/ducklake/neon_<site>.ducklake)
ATTACH IF NOT EXISTS 'ducklake:sites/SRER/ducklake/neon_srer.ducklake' AS neon_srer
    (READ_ONLY, DATA_PATH 'sites/SRER/ducklake/data/', OVERRIDE_DATA_PATH true);

ATTACH IF NOT EXISTS 'ducklake:sites/HARV/ducklake/neon_harv.ducklake' AS neon_harv
    (READ_ONLY, DATA_PATH 'sites/HARV/ducklake/data/', OVERRIDE_DATA_PATH true);
