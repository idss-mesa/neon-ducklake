---
type: Reference
title: "For AI agents"
description: "How agents should consume the neon-ducklake documentation (llms.txt, per-page Markdown with OKF frontmatter, raw GitHub sources, trust signals) and how to get the data itself: attach the public lakes read-only with DuckDB, or use neon-mcp for the live NEON API."
tags:
  - about
  - ai-agents
  - OKF
  - llms.txt
generated:
  by: "claude/opus-5.5"
  at: "2026-11-24T00:00:00Z"
sources:
  - id: okf-spec
    resource: "https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md"
    title: "Open Knowledge Format (OKF) v0.2 specification"
    author: "team:google-cloud"
  - id: llmstxt
    resource: "https://llmstxt.org"
    title: "The /llms.txt convention"
    author: "team:answer-ai"
  - id: neon-mcp
    resource: "https://idss-mesa.github.io/neon-mcp/"
    title: "neon-mcp documentation"
    author: "team:idss-mesa"
status: stable
stale_after: "2027-05-24T00:00:00Z"
---

# For AI agents

This site is published for people and for AI agents. The documentation source is an
[Open Knowledge Format (OKF) v0.2](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
bundle, and the deployed site exposes that structure directly.

## If you want the data

neon-ducklake has no server of its own. Its lakes are public files, and DuckDB reads them in place:

- **What NEON data exists, where and when:** attach the [catalog lake](../data/catalog-lake.md)
  read-only over HTTPS and query `v_site_inventory`, `availability_months`, `files`, `products`
  and `sites`.
- **Stacked measurements:** attach a [site lake](../data/site-lakes.md). Only SRER and KONZ are
  published, both as partial sample runs; check `site_months` before claiming coverage.
- **Who set which metadata, and when:** read the [AVU history](../data/avu-history.md) with
  `read_parquet`.
- **Anything newer than the 2026-09-27 harvest, or a download link for a file:** use the live
  NEON API, for example through [neon-mcp](https://idss-mesa.github.io/neon-mcp/)
  (`neon_get_availability`, `neon_list_files`; file listings need a NEON API token).

Every SQL example on the dataset cards was run against the public collection on 2026-11-24, and
its output excerpt is copied from that run. Prefer running a query to quoting a number from prose;
the lakes change when the pipeline runs again.

## Entry points

All URLs are under `https://idss-mesa.github.io/neon-ducklake/`.

| Endpoint | What you get |
|---|---|
| [`llms.txt`](https://idss-mesa.github.io/neon-ducklake/llms.txt) | Linked outline of every page with one-line descriptions ([llms.txt convention](https://llmstxt.org)) |
| [`llms-full.txt`](https://idss-mesa.github.io/neon-ducklake/llms-full.txt) | Every page's Markdown with frontmatter in one file, each prefixed by its canonical URL |
| Any page URL + `index.md` | That page's Markdown source with OKF frontmatter, for example `https://idss-mesa.github.io/neon-ducklake/data/catalog-lake/index.md` |
| Any page path + `.md` | The same Markdown, for example `https://idss-mesa.github.io/neon-ducklake/data/catalog-lake.md` |
| Raw source on GitHub | `https://raw.githubusercontent.com/idss-mesa/neon-ducklake/main/docs/<path>.md`; reachable from sandboxes that allow `github.com` but not `*.github.io` |
| [`agent-questions.yml`](https://github.com/idss-mesa/neon-ducklake/blob/main/docs/agent-questions.yml) | The questions this documentation is meant to answer, with reference answers checked against the code and the data |
| [Source repository](https://github.com/idss-mesa/neon-ducklake) | The bundle under `docs/`, plus `AGENTS.md` for coding agents |

Every rendered page also has a visible "View this page as Markdown" button and a
"Machine-readable versions" line, because `<head>` tags are invisible to most fetch tools.

## If you cannot fetch this site

1. Use the raw source: `docs/` mirrors the site paths one to one on branch `main`.
   `https://idss-mesa.github.io/neon-ducklake/data/site-lakes/` is
   `https://raw.githubusercontent.com/idss-mesa/neon-ducklake/main/docs/data/site-lakes.md`.
2. Prefer one fetch over ten: `llms-full.txt` holds every page.
3. Avoid the GitHub tree API unless authenticated; raw file paths are not rate-limited the same way.

## Reading the OKF frontmatter

- **What is this?** `type` (`Dataset` for the dataset cards; also `Reference`, `Guide`,
  `Architecture`, `Explanation`, `Design Record`, `Runbook`), `title`, `description`, `tags`.
- **Where did it come from?** `generated: { by, at }` and `sources`. The dataset cards cite the
  published collection itself and the code that writes it.
- **How much should I trust it?** The `verified` key. Its absence means nobody but the generator
  has confirmed the page. Every page here starts unverified.
- **Is it still true?** `status` and `stale_after`. Dataset cards go stale on 2027-02-15. Every NEON release
  in the lake was generated in late January (2021 to 2026), so a new release may exist by then
  that the catalog lake does not reflect until it is rebuilt.

## Answering questions

Cite the page URL. When a page and the code disagree, the code in the
[repository](https://github.com/idss-mesa/neon-ducklake) wins, and the page is due for an update.
When a page and the data disagree, the data wins. Report gaps on the
[issue tracker](https://github.com/idss-mesa/neon-ducklake/issues).

## Related bundles

- **MESA project site:** [`llms.txt`](https://idss-mesa.github.io/llms.txt) for
  https://idss-mesa.github.io/.
- **neon-mcp:** [agent guide](https://idss-mesa.github.io/neon-mcp/about/ai-agents/) for the MCP
  server for the NEON Data API.
- **mesa-ducklake:** the library that writes the AVU history,
  [github.com/idss-mesa/mesa-ducklake](https://github.com/idss-mesa/mesa-ducklake).
