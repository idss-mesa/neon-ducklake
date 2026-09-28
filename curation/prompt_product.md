You are annotating ONE NEON data product with ontology-grounded metadata for the MESA DuckLake catalog. Metadata are iRODS-style AVUs (attribute | value | unit). The dataset card for the product follows the "DATASET CARD" heading at the end.

GOAL: Produce 8–20 AVUs that describe WHAT this product is about, for discovery and reuse, grounded ONLY in OBO Foundry ontologies (for example PATO qualities, ENVO environments and materials, OBI assays, devices and protocols, IAO information artifacts, GO/ENVO processes, NCBITaxon organism groups, PCO population and community concepts, BCO biodiversity collection concepts, CHEBI chemical entities, OBA biological attributes).

The annotations are product-level and generic: the same AVUs are reused for this product at every NEON site. Cover only these aspects, as they apply:
- organism_group: the organism group(s) the product observes. Use HIGH-LEVEL NCBITaxon terms only (for example Aves, Insecta, Poaceae, Bacteria, Fungi), never a species or anything below species.
- measured_property: what is measured or observed, as a quantity or quality (PATO, OBA, ENVO, …).
- environmental_material: the environmental material or feature sampled or measured (ENVO), for example air, soil, surface water, tree canopy.
- method: the assay, method, device or protocol used (OBI, …), for example point count, eddy covariance, thermometer.
- data_type: the kind of data or record (IAO, OBI, …), for example time series, measurement datum, observation record.
- process: a biological or environmental process the product measures (GO, ENVO, …), for example photosynthesis, evaporation.

FORBIDDEN (these come from the data itself, and anything like them will be rejected):
- species-level (or lower) taxa, and lists of individual species;
- units of measure (UO) — never add a UO term;
- biomes or ecoregions (ENVO biome terms), land-cover classes;
- site names, place names or site codes (GAZ, "SRER", "Harvard Forest", …);
- non-OBO vocabularies, and terms from reserved schemas (datacite, dc, eml).

RULES
1. Every AVU must come from a real term you found with the tools. Use mesa_ols_search_terms to find candidates (restrict ontology_id when you know it, for example "pato", "envo", "obi", "iao", "ncbitaxon", "go"; keep size ≤ 10), optionally mesa_ols_get_term to confirm a term, then mesa_avu_from_term to build the AVU. Never invent a CURIE, IRI or label, and never use an ID from memory without finding it in a tool result.
2. The `value` you pass to mesa_avu_from_term is the dataset-specific value. Usually it is the term label. It must not name a site.
3. Prefer the most specific correct term that is true for the whole product. Do not add two terms that say the same thing.
4. `column` is the exact name of a data column from the card that the term describes (for example "tempSingleMean"), or null. Only name columns that appear in the card.
5. Use at most 30 tool calls in total.
6. When finished, reply with ONLY a JSON object (no prose, no code fence) of this shape:
{"avus": [{"attribute": "...", "value": "...", "unit": "<CURIE>", "ontology_id": "...", "curie": "...", "iri": "...", "label": "...", "aspect": "organism_group|measured_property|environmental_material|method|data_type|process", "column": "<column name or null>", "rationale": "<≤20 words>"}]}
Copy attribute/value/unit exactly from mesa_avu_from_term's `avu`, and ontology_id/curie/iri/label from its `term`.

DATASET CARD
