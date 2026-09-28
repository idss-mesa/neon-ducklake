You are choosing the ENVO environment terms of ONE NEON field site for the MESA DuckLake catalog. The site card follows the "SITE CARD" heading at the end: the site's name, NEON HABITAT class, domain, state, location, description and data products, plus a heuristic proposal that is only a hint.

GOAL: Choose, from ENVO (the Environment Ontology, an OBO Foundry ontology):
- biome: exactly ONE ENVO biome term (a descendant of ENVO:00000428 'biome') that best describes the ecosystem the site's data were collected in.
- feature: for an AQUATIC site only (HABITAT starts with "Aquatic"), exactly ONE ENVO environmental feature for the water body that is sampled (a descendant of ENVO:00000063 'water body', for example a stream, river, lake or pond). A terrestrial site gets no feature.

HOW TO DECIDE
- HABITAT is authoritative. An aquatic site's biome must be an aquatic (freshwater) biome, never a terrestrial one such as a grassland or forest biome, whatever the surrounding land looks like.
  - Aquatic Wadeable Stream: a stream; a freshwater stream biome (or a river biome only when the card shows the site is a river).
  - Aquatic Non-wadeable Stream: a river; a freshwater river biome.
  - Aquatic Lake: a lake (or a pond when the card shows it is one); a freshwater lake biome.
- For a terrestrial site, use the TypeEco, the NLCD land-cover shares, the domain, the location (latitude, elevation) and the description. Prefer the most specific biome that is true for the whole site (for example "temperate grassland biome" over "grassland biome"), but do not guess a subtype the card does not support.
- The heuristic proposal may be wrong. Check it against the card and choose independently.

RULES
1. Every term must be a real ENVO term you found with the tools. For each role, first search for candidates with mesa_ols_search_terms (ontology_id "envo", size ≤ 10), at least once and without relying on the heuristic's CURIEs, then confirm the term you choose with mesa_ols_get_term. Never invent a CURIE or label, and never use an ID from memory without finding it in a tool result.
2. Use terms that are not obsolete. Do not use land-cover classes ("area of …"), ecoregions, materials or places.
3. Use at most 12 tool calls in total.
4. When finished, reply with ONLY a JSON object (no prose, no code fence) of this shape:
{"terms": [{"role": "biome", "curie": "ENVO:…", "label": "<the ENVO label exactly as OLS gives it>", "rationale": "<≤25 words>"}, {"role": "feature", "curie": "ENVO:…", "label": "…", "rationale": "…"}]}
A terrestrial site's list has only the biome item.

SITE CARD
