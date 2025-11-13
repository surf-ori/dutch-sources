# Agents & Automation Notes

The notebook is structured like a small team of agents so each responsibility is easy to re-run independently. Use this cheat-sheet when scripting or delegating the work outside Jupyter.

| Agent | Cells | Responsibilities | Hand-off artifacts |
|-------|-------|------------------|--------------------|
| **Baseline Scout** | 5–9 | Downloads the NL baseline sheet, normalises identifiers, and enriches missing OpenAIRE IDs via the ROR lookups. | `data/nl_orgs_baseline.xlsx`, in-memory `universities_df`. |
| **Metrics Harvester** | 10–14 | Runs sample sanity checks, enriches all orgs, and keeps `enriched_df` + PNG visualisations up-to-date. | `data/nl_orgs_openaire.xlsx`, `img/org_total_products.png`. |
| **Datasource Cartographer** | 16 | Pulls every datasource linked to the orgs (including compatibility labels). | `data/nl_orgs_openaire_datasources.xlsx`. |
| **Snapshot Scribe** | 18–20 | Captures per-datasource `numFound` totals, writes dated Excel snapshots, and appends them to the rolling history workbook. | `data/nl_orgs_openaire_datasources_numFound_YYYY-MM-DD.xlsx`, `data/nl_orgs_openaire_datasources_numFound_history.xlsx`. |
| **Endpoint Collector** | 14 | Downloads the curated OAI-PMH spreadsheet so the latest endpoints are on disk. | `data/curated_oai_endpoints.xlsx`. |
| **Endpoint Backfiller** | 15 | Uses the curated endpoint list to inject `OAI-endpoint` values back into the datasource export. | `data/nl_orgs_openaire_datasources_with_endpoint.xlsx`. |
| **Endpoint Auditor** | 16 | Probes each endpoint with `ListMetadataFormats` to record actual metadata support and OAI health. | `data/nl_orgs_openaire_datasources_with_endpoint_metrics.xlsx`. |
| **Viz Painter** | 17 | Builds the endpoint diagnostics summary table + two PNG charts for documentation. | `img/oai_endpoint_summary.png`, `img/oai_openaire_compatibility_by_type.png`. |
| **Dashboard Guide** | 18 | Loads org, history, and endpoint metrics into an interactive Plotly/widget dashboard for stakeholder self-service. | live widget output in `overview-stats.ipynb`. |

## When to Run What
- **Daily/Weekly checks**: Run the Snapshot Scribe and Viz Painter cells to refresh the history workbook and PNGs.
- **After baseline changes**: Start from the Baseline Scout, then re-run the Harvester, Cartographer, and Scribe to keep downstream artifacts consistent.
- **Headless automation**: The logic inside each “agent” cell block is copy/paste ready for a CLI script—just make sure `.env` is loaded and `/data` + `/img` exist.

Keeping the responsibilities split like this makes it easy to rerun the minimal amount of work (and shortens re-review cycles when OpenAIRE tweaks the API schemas).
