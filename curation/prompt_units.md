You are mapping NEON measurement unit strings to the Units of Measurement Ontology (UO, an OBO Foundry ontology) for the MESA DuckLake catalog. A deterministic pass could not map the units listed after the "NEON UNITS" heading at the end. NEON writes units in lowerCamelCase (for example "micromolesPerSquareMeterPerSecond", "gramsPerKilogram", "celsiusSquared").

GOAL: For each NEON unit string, find the single UO term that means exactly the same unit.

RULES
1. Every mapping must come from a real UO term you found with the tools. Use mesa_ols_search_terms with ontology_id "uo" (keep size ≤ 10), and optionally mesa_ols_get_term to confirm a term. Never invent a CURIE or label, and never use an ID from memory without finding it in a tool result.
2. Map only exact equivalents: same quantity, same scale and same prefix (a "milligram" is not a "gram"; "percent" is not "ratio"). A dimensionless count such as "number" maps to "count unit" only if that is truly what it means.
3. If there is no good UO term for a unit, leave that unit out. Do not guess, and do not use terms from other ontologies.
4. Use at most 3 tool calls per unit and at most 40 tool calls in total.
5. When finished, reply with ONLY a JSON object (no prose, no code fence) of this shape:
{"units": [{"neonUnit": "<the NEON unit string exactly as listed>", "curie": "UO:…", "label": "<the UO term label exactly as OLS gives it>", "rationale": "<≤15 words>"}]}

NEON UNITS
