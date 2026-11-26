#!/usr/bin/env python3
"""Post-build step: make a rendered Zensical docs site consumable by AI agents.

Run AFTER `zensical build --clean --strict`. It:

1. Mirrors every source Markdown file — OKF v0.2 frontmatter intact — into
   the built site at each page's pretty URL:
       docs/a/b.md        -> site/a/b/index.md      (page URL + "index.md")
       docs/a/index.md    -> site/a/index.md
       docs/index.md      -> site/index.md
   so any agent can turn a page URL into its canonical Markdown by appending
   `index.md` (e.g. https://idss-mesa.github.io/docs/quickstart/index.md).
   Relative links in the mirror are rewritten to absolute URLs, since the
   mirror sits one directory deeper than its source. It also keeps the legacy
   `docs/a/b.md -> site/a/b.md` copy that the earlier agent_markdown.py
   published, so existing links still resolve. Source files are never changed.
2. Injects agent-discoverable metadata into each page's <head>:
       <meta name="robots" ...>
       <link rel="alternate" type="text/markdown" href="index.md">
       <meta name="okf:type" | okf:status | okf:trust-tier | okf:generated-at
             | okf:generated-by | okf:stale-after>
3. Adds two *visible* pointers to every page, because text-extracting fetch
   tools and link-derived URL allowlists never see <head>: a "View this page
   as Markdown" button beside "View source", and a "Machine-readable
   versions" line at the end of the article linking the Markdown twin, the
   raw GitHub source, llms.txt, and llms-full.txt.
4. Writes robots.txt advertising sitemap.xml, llms.txt, llms-full.txt, the
   Markdown mirror and raw-source conventions, and the agent guide. Crawlers
   read robots.txt only at the origin root
   (https://idss-mesa.github.io/robots.txt, from the idss-mesa.github.io
   repository), so this copy is a pointer for harnesses.
5. Deletes the zero-byte objects.inv stub Zensical emits.

Site settings come from zensical.toml. Needs PyYAML and Python 3.11+.

Usage: python scripts/postbuild_agent_surface.py [site_dir]   (from the repository root)
"""

from __future__ import annotations

import html
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from okf_common import (DOCS, ROOT, absolutize, load_config,  # noqa: E402
                        page_url, raw_source_url, rewrite_link_targets,
                        site_url, split_frontmatter)

AI_AGENTS = ["Googlebot", "Google-Extended", "GoogleOther", "Google-CloudVertexBot",
             "GPTBot", "OAI-SearchBot", "ChatGPT-User",
             "ClaudeBot", "Claude-User", "Claude-SearchBot", "PerplexityBot",
             "cohere-ai", "Applebot-Extended", "CCBot", "meta-externalagent",
             "Amazonbot", "DuckAssistBot", "MistralAI-User"]


def trust_tier(fm: dict) -> str:
    """OKF §5.3: unverified | machine-confirmed | human-reviewed."""
    v = fm.get("verified")
    if not v:
        return "unverified"
    entries = v if isinstance(v, list) else [v]
    actors = [str(e.get("by", "")) for e in entries if isinstance(e, dict)]
    if any(a.startswith("human:") for a in actors):
        return "human-reviewed"
    return "machine-confirmed" if actors else "unverified"


def head_block(fm: dict) -> str:
    lines = ['<meta name="robots" content="index, follow, max-snippet:-1, '
             'max-image-preview:large, max-video-preview:-1">',
             '<link rel="alternate" type="text/markdown" '
             'title="Markdown source (OKF v0.2 frontmatter)" href="index.md">']

    def meta(name, value):
        if value:
            lines.append(f'<meta name="{name}" content="{html.escape(str(value), quote=True)}">')

    meta("okf:type", fm.get("type"))
    meta("okf:status", fm.get("status", "stable") if fm else None)
    meta("okf:trust-tier", trust_tier(fm) if fm else None)
    gen = fm.get("generated") or {}
    if isinstance(gen, dict):
        meta("okf:generated-at", gen.get("at"))
        meta("okf:generated-by", gen.get("by"))
    meta("okf:stale-after", fm.get("stale_after"))
    return "\n".join(lines) + "\n"


MD_ICON = ('<svg xmlns="http://www.w3.org/2000/svg" fill="none" stroke="currentColor" '
           'stroke-linecap="round" stroke-linejoin="round" stroke-width="2" '
           'class="lucide lucide-file-text" viewBox="0 0 24 24" aria-hidden="true">'
           '<path d="M15 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7Z"/>'
           '<path d="M14 2v4a2 2 0 0 0 2 2h4M10 9H8M16 13H8M16 17H8"/></svg>')


def markdown_button() -> str:
    return ('<a href="index.md" title="View this page as Markdown (for AI agents and '
            'screen readers)" class="md-content__button md-icon" '
            'type="text/markdown">' + MD_ICON + '</a>\n')


def machine_readable_line(url: str, raw: str | None, base: str) -> str:
    parts = [f'<a href="{url}index.md" type="text/markdown">Markdown twin</a>']
    if raw:
        parts.append(f'<a href="{raw}">raw source on GitHub</a>')
    parts += [f'<a href="{base}llms.txt">llms.txt</a>',
              f'<a href="{base}llms-full.txt">llms-full.txt (whole site)</a>']
    return ('<p class="mesa-machine-readable">Machine-readable versions of this page: '
            + " · ".join(parts) + '. See <a href="' + base
            + 'about/ai-agents/">For AI agents</a>.</p>\n')


def dest_for(rel: Path, site: Path) -> Path:
    if rel.name == "index.md":
        return site / rel
    return site / rel.parent / rel.stem / "index.md"


def main():
    site = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "site"
    if not site.is_dir():
        print(f"error: {site} not found — run `zensical build` first", file=sys.stderr)
        sys.exit(2)
    cfg = load_config()
    base = site_url(cfg)
    name = cfg.get("site_name", "Documentation")

    # 0. Zensical writes an empty objects.inv (a Sphinx inventory stub). No
    # consumer reads it and a zero-byte file only confuses crawlers, so drop it.
    inv = site / "objects.inv"
    if inv.exists() and inv.stat().st_size == 0:
        inv.unlink()

    # 1. Mirror Markdown sources at pretty URLs (plus the legacy <path>.md
    # copy), with relative links rewritten to absolute URLs.
    mirrored = 0
    fms: dict[Path, dict] = {}
    rel_of: dict[Path, str] = {}
    for path in sorted(DOCS.rglob("*.md")):
        rel = path.relative_to(DOCS)
        if rel.parts[0] in ("assets", "stylesheets"):
            continue
        text = path.read_text(encoding="utf-8")
        try:
            fm, body = split_frontmatter(text)
        except ValueError:
            fm, body = None, text
        head = text[: len(text) - len(body)]
        rel_posix = rel.as_posix()
        body = rewrite_link_targets(body, lambda t, r=rel_posix: absolutize(t, r, base))
        dest = dest_for(rel, site)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(head + body, encoding="utf-8")
        legacy = site / rel
        if legacy != dest:
            legacy.parent.mkdir(parents=True, exist_ok=True)
            legacy.write_text(head + body, encoding="utf-8")
        fms[dest.parent.resolve()] = fm or {}
        rel_of[dest.parent.resolve()] = rel_posix
        mirrored += 1

    # 2. Inject <head> metadata and the visible pointers into each page whose
    # directory has a mirror.
    injected = 0
    for htmlfile in sorted(site.rglob("index.html")):
        key = htmlfile.parent.resolve()
        fm = fms.get(key)
        if fm is None:
            continue
        text = htmlfile.read_text(encoding="utf-8")
        if 'rel="alternate" type="text/markdown"' in text:
            continue  # idempotent
        text = text.replace("</head>", head_block(fm) + "</head>", 1)

        # Visible pointers: body text survives text extraction, <head> does not.
        marker = 'title="View source of this page" class="md-content__button md-icon">'
        if marker in text:
            end = text.index("</a>", text.index(marker)) + len("</a>")
            text = text[:end] + "\n" + markdown_button() + text[end:]
        elif "<article" in text:  # no repo buttons configured: lead the article
            open_end = text.index(">", text.index("<article")) + 1
            text = text[:open_end] + "\n" + markdown_button() + text[open_end:]
        rel_posix = rel_of.get(key, "")
        if rel_posix and "</article>" in text:
            line = machine_readable_line(page_url(base, rel_posix),
                                         raw_source_url(cfg, rel_posix), base)
            text = text.replace("</article>", line + "</article>", 1)

        htmlfile.write_text(text, encoding="utf-8")
        injected += 1

    # 3. robots.txt — explicitly welcome AI fetchers alongside the blanket allow.
    ai_block = "".join(f"User-agent: {a}\nAllow: /\n\n" for a in AI_AGENTS)
    raw_root = raw_source_url(cfg, "")
    raw_line = (f"#   Raw source on GitHub:      {raw_root}<path>.md\n"
                if raw_root else "")
    (site / "robots.txt").write_text(
        f"# {name} documentation — {base}\n"
        "# This documentation is published for people AND for AI agents.\n"
        "# Crawlers read robots.txt only at the origin root; the equally\n"
        "# permissive https://idss-mesa.github.io/robots.txt governs this\n"
        "# sub-site. This copy repeats the agent pointers for harnesses.\n"
        "User-agent: *\n"
        "Allow: /\n"
        "\n"
        + ai_block +
        f"Sitemap: {base}sitemap.xml\n"
        "\n"
        "# AI agents and harnesses:\n"
        f"#   Machine-readable outline:  {base}llms.txt\n"
        f"#   Full corpus (one file):    {base}llms-full.txt\n"
        "#   Markdown source of any page (OKF v0.2 frontmatter: type, provenance,\n"
        "#   trust, lifecycle): append `index.md` to the page URL, or use the\n"
        "#   \"View this page as Markdown\" button on the page itself.\n"
        + raw_line +
        f"#   Agent guide:               {base}about/ai-agents/\n"
        "#   Origin index:              https://idss-mesa.github.io/llms.txt\n"
        "#   For CyVerse data itself, install or connect to the MESA MCP\n"
        "#   servers rather than scraping these pages.\n",
        encoding="utf-8")

    print(f"agent surface: mirrored {mirrored} markdown files, "
          f"annotated {injected} pages, wrote robots.txt")


if __name__ == "__main__":
    main()
