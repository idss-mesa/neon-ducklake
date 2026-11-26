#!/usr/bin/env python3
"""Generate llms.txt and llms-full.txt from a docs/ OKF bundle.

llms.txt      — linked outline of the site (llmstxt.org convention): every
                page with its frontmatter description, grouped by the nav
                sections in zensical.toml. Each entry carries three addresses
                that return the same content — the rendered page, its Markdown
                twin (page URL + `index.md`), and the raw source on GitHub —
                because sandboxed agents often reach raw.githubusercontent.com
                when they cannot reach *.github.io.
llms-full.txt — the entire corpus concatenated as Markdown, frontmatter
                included and relative links rewritten to absolute URLs, so an
                agent can ingest the whole bundle in one fetch.

Site name, description, URL, repository, and page order all come from
zensical.toml. Both files are written into docs/ so the static build ships
them at the site root; CI regenerates them and fails if the committed copies
drift.

Usage: python scripts/gen_llms_txt.py      (from the repository root)
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from okf_common import (DOCS, absolutize, frontmatter, is_external,  # noqa: E402
                        load_config, nav_pages, page_url, raw_source_url,
                        repo_branch, rewrite_link_targets, site_url,
                        split_frontmatter)

# Heading for pages that sit directly in the nav rather than inside a section.
# Overridable with [project.extra] llms_top_heading in zensical.toml.
TOP_LEVEL_HEADING = "Overview"

ORIGIN_LLMS = "https://idss-mesa.github.io/llms.txt"


def main() -> int:
    cfg = load_config()
    base = site_url(cfg)
    name = cfg.get("site_name", "Documentation")
    desc = cfg.get("site_description", "").strip()
    # One or two sentences on what the documentation covers, from
    # [project.extra] llms_scope in zensical.toml; optional.
    scope = str(cfg.get("extra", {}).get("llms_scope", "")).strip()
    scope = scope + " " if scope else ""
    raw_root = raw_source_url(cfg, "")
    top_heading = str(cfg.get("extra", {}).get("llms_top_heading", TOP_LEVEL_HEADING))
    # A real page for the URL-to-raw-source example line.
    example = next((t[: -len(".md")] for _, _, t in nav_pages(cfg.get("nav", []))
                    if not is_external(t) and t.endswith(".md") and t != "index.md"
                    and not t.endswith("/index.md") and t != "log.md"), "page")
    branch = repo_branch(cfg)

    lines = [
        f"# {name} documentation",
        "",
        f"> {desc} {scope}The source repository is an Open Knowledge Format "
        "(OKF v0.2) bundle: every page carries YAML frontmatter with type, "
        "provenance (generated/sources), and lifecycle (status/stale_after) "
        "fields.",
        "",
        "Every page below is listed with three addresses that all return the "
        "same content: the rendered HTML page, its Markdown twin (page URL + "
        "`index.md`, served as text/markdown with the OKF frontmatter), and "
        "the raw source file on GitHub. Fetch whichever your tool is allowed "
        "to reach; many sandboxes permit github.com and "
        "raw.githubusercontent.com but not *.github.io.",
        "",
        "```",
        f"Site page        {base}<path>/",
        f"Markdown twin    {base}<path>/index.md",
    ]
    if raw_root:
        lines += [
            f"Raw source       {raw_root}<path>.md      (branch {branch}; a moving target)",
            "",
            f"Content page     /{example}/  ->  {raw_root}{example}.md",
        ]
    lines += ["```", "",
              f"The legacy form {base}<path>.md also still returns the same "
              "Markdown, for links made before 2026-09-10.", ""]

    full = [
        f"# {name} documentation — full corpus",
        "",
        "Each page below begins with its canonical URL followed by its "
        "original Markdown, OKF frontmatter included. Relative links have "
        "been rewritten to absolute URLs.",
        "",
    ]

    # section -> subsection -> outline lines, in nav order; empty groups are
    # never rendered.
    groups: dict[str, dict[str | None, list[str]]] = {}

    def add(trail: tuple, line: str):
        section = trail[0] if trail else top_heading
        sub = " / ".join(trail[1:]) or None
        groups.setdefault(section, {}).setdefault(sub, []).append(line)

    seen: set[str] = set()
    n = 0
    for trail, label, target in nav_pages(cfg.get("nav", [])):
        if is_external(target):
            add(trail, f"- [{label or target}]({target}): External resource.")
            continue
        rel = target.replace("\\", "/")
        if rel in seen or not (DOCS / rel).exists():
            continue
        seen.add(rel)
        # Section listings (OKF §8) and the §9 log are not concept pages; the
        # bundle root index.md is this site's homepage, so it stays listed.
        if rel.endswith("/index.md") or rel == "log.md":
            continue
        fm = frontmatter(DOCS / rel)
        url = page_url(base, rel)
        title = fm.get("title") or label or Path(rel).stem.replace("-", " ").title()
        summary = str(fm.get("description", "")).strip()
        suffix = ""
        if fm.get("status") == "deprecated":
            suffix = " (deprecated; kept for history)"
        elif fm.get("status") == "draft":
            suffix = " (draft)"
        raw = raw_source_url(cfg, rel)
        alt = f" Markdown twin: {url}index.md" + (f" Raw source: {raw}" if raw else "")
        add(trail, f"- [{title}]({url}): {summary}{suffix}{alt}")
        text = (DOCS / rel).read_text(encoding="utf-8")
        _, body = split_frontmatter(text)
        head = text[: len(text) - len(body)]
        body = rewrite_link_targets(body, lambda t, r=rel: absolutize(t, r, base))
        full += [f"---8<--- {url}", "", (head + body).rstrip(), ""]
        n += 1

    for section, subs in groups.items():
        lines += [f"## {section}", ""]
        for sub, entries in subs.items():
            if sub:
                lines += [f"### {sub}", ""]
            lines += entries + [""]

    log = DOCS / "log.md"
    if log.exists():
        full += [f"---8<--- {base}log/", "", log.read_text(encoding="utf-8").rstrip(), ""]
    full_text = "\n".join(full).rstrip() + "\n"
    nbytes = len(full_text.encode("utf-8"))
    ktok = max(1, round(nbytes / 4 / 1000))

    lines += [
        "## Meta",
        "",
        f"- [Full corpus in one file]({base}llms-full.txt): every page's Markdown with "
        f"frontmatter, links made absolute; about {nbytes // 1024} KB, roughly {ktok},000 "
        "tokens. Prefer it over fetching pages one at a time.",
        f"- [Documentation update log]({base}log/): dated history of changes to this bundle.",
        f"- [For AI agents]({base}about/ai-agents/): endpoints, trust signals, and what to "
        "do if you cannot fetch this site.",
        f"- [Sitemap]({base}sitemap.xml) and [robots.txt]({base}robots.txt): crawlers honour "
        "only the origin https://idss-mesa.github.io/robots.txt, which is equally permissive.",
        f"- [MESA project site]({ORIGIN_LLMS}): origin-level llms.txt indexing the MESA site, "
        "this documentation, and the MESA software repositories.",
    ]
    if raw_root:
        lines.append(f"- [Source repository]({cfg.get('repo_url')}): the bundle itself; "
                     "`docs/` mirrors the site paths one to one.")
    lines += ["",
              "If you want CyVerse data rather than documentation, install the MESA MCP "
              "servers (or connect to a hosted endpoint) and call their tools; the agent "
              "guide explains how.", ""]

    (DOCS / "llms.txt").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    (DOCS / "llms-full.txt").write_text(full_text, encoding="utf-8")
    print(f"llms.txt: {n} pages indexed; llms-full.txt: "
          f"{(DOCS / 'llms-full.txt').stat().st_size // 1024} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
