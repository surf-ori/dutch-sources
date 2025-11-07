# Agents & Automation Notes

The notebook is structured like a small team of agents so each responsibility is easy to re-run independently. Use this cheat-sheet when scripting or delegating the work outside Jupyter.

| Agent | Cells | Responsibilities | Hand-off artifacts |
|-------|-------|------------------|--------------------|
| **Baseline Scout** | 5–9 | Downloads the NL baseline sheet, normalises identifiers, and enriches missing OpenAIRE IDs via the ROR lookups. | `data/nl_orgs_baseline.xlsx`, in-memory `universities_df`. |
| **Metrics Harvester** | 10–14 | Runs sample sanity checks, enriches all orgs, and keeps `enriched_df` + PNG visualisations up-to-date. | `data/nl_orgs_openaire.xlsx`, `img/org_total_products.png`. |
| **Datasource Cartographer** | 16 | Pulls every datasource linked to the orgs (including compatibility labels). | `data/nl_orgs_openaire_datasources.xlsx`. |
| **Snapshot Scribe** | 18–20 | Captures per-datasource `numFound` totals, writes dated Excel snapshots, and appends them to the rolling history workbook. | `data/nl_orgs_openaire_datasources_numFound_YYYY-MM-DD.xlsx`, `data/nl_orgs_openaire_datasources_numFound_history.xlsx`. |
| **Viz Painter** | 22–24 | Generates the latest datasource overview chart plus the latest-vs-previous comparison, saving both to `/img`. | `img/datasource_totals_latest.png`, `img/datasource_totals_compare.png`. |

## When to Run What
- **Daily/Weekly checks**: Run the Snapshot Scribe and Viz Painter cells to refresh the history workbook and PNGs.
- **After baseline changes**: Start from the Baseline Scout, then re-run the Harvester, Cartographer, and Scribe to keep downstream artifacts consistent.
- **Headless automation**: The logic inside each “agent” cell block is copy/paste ready for a CLI script—just make sure `.env` is loaded and `/data` + `/img` exist.

Keeping the responsibilities split like this makes it easy to rerun the minimal amount of work (and shortens re-review cycles when OpenAIRE tweaks the API schemas).
