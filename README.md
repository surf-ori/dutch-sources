# Dutch Sources – CRIS and Repository Metrics Toolkit

This repository hosts a reproducible workflow for monitoring how Dutch research organisations and their associated data sources appear inside the [OpenAIRE Graph](https://graph.openaire.eu/). The workflow is implemented as a Jupyter notebook (`overview-stats.ipynb`) and produces both analytical datasets and visual reports suitable for stakeholders who oversee harvesting quality.

---

## Table of Contents

1. [Features](#features)
2. [Repository Layout](#repository-layout)
3. [Prerequisites](#prerequisites)
4. [Quick Start](#quick-start)
5. [Generated Artifacts](#generated-artifacts)
6. [Visual Reports](#visual-reports)
7. [Operational Notes](#operational-notes)
8. [Troubleshooting](#troubleshooting)
9. [Contributing](#contributing)

---

## Features

- **Automated baseline retrieval** – downloads the latest NL organisational spreadsheet directly from Google Sheets and normalises identifiers (ROR, OpenAIRE, CRIS, repository).
- **Authenticated API access** – uses the OpenAIRE client-credentials grant to take advantage of higher rate limits.
- **Concurrent harvesting** – leverages thread pools and progress bars for ID enrichment, scenario metrics, and datasource snapshots.
- **Historical tracking** – appends every datasource “numFound” snapshot to an Excel history file, enabling longitudinal analysis.
- **Ready-to-share graphics** – saves publication and datasource coverage charts into `img/` for re-use in presentations or dashboards.
- **Streamlit dashboard explorer** – a dedicated `streamlit_app.py` provides multi-select filters, numeric range sliders, quick “error focus” presets, and a responsive chart grid sourced from the exported dashboard dataset.

---

## Repository Layout

```code
overview-stats.ipynb   # Main notebook with numbered sections (baseline → metrics → visualisations)
.env.example           # Template for CLIENT_ID and CLIENT_SECRET
data/                  # Auto-generated CSV/Excel outputs (ignored by git)
img/                   # Auto-generated PNG charts (checked in for documentation)
agents.md              # Notes describing each automation “agent” inside the notebook
```

---

## Prerequisites

- Python 3.10+
- Streamlit 1.30+ (for the standalone dashboard)
- OpenAIRE AAI client credentials with access to the Graph APIs [https://develop.openaire.eu/apis]
- Ability to open the published Google Sheet referenced in the notebook [nl-orgs-baseline](https://docs.google.com/spreadsheets/d/1s2eeEBGR9ovKkFZMvqNQHPXqLQB_GH55/edit?usp=sharing&ouid=117254663676107205045&rtpof=true&sd=true)
- JupyterLab (or another Jupyter interface)

---

## Quick Start

```bash
git clone https://github.com/surf-ori/dutch-sources.git
cd dutch-sources
python3 -m venv .venv && source .venv/bin/activate
cp .env.example .env   # fill in CLIENT_ID and CLIENT_SECRET
jupyter lab
```

Open `overview-stats.ipynb` and execute the cells in order. The first cell installs any missing Python packages (`pandas`, `matplotlib`, `openpyxl`, `requests`, `python-dotenv`, `tqdm`, `pyarrow`). Each numbered section in the notebook is self-contained and can be re-run independently when you only need part of the pipeline. After running the dashboard export cell (Step 15), start the web dashboard with:

```bash
streamlit run streamlit_app.py
```

The Streamlit app reads `data/nl_orgs_dashboard_data.xlsx`, so rerun the notebook cell whenever fresh snapshots are captured.

---

## Generated Artifacts

| Artifact | Location | Purpose |
| --- | --- | --- |
| `nl_orgs_baseline.xlsx` | `data/` | Latest curated NL baseline sheet downloaded from Google Sheets. |
| `nl_orgs_openaire.xlsx` | `data/` | Baseline enriched with OpenAIRE IDs and per-organisation metrics. |
| `nl_orgs_openaire_datasources.xlsx` | `data/` | Registry metadata for every datasource linked to the organisations. |
| `nl_orgs_openaire_datasources_numFound_<date>.xlsx` | `data/` | Daily snapshot of total and per-type counts per datasource. |
| `nl_orgs_openaire_datasources_numFound_history.xlsx` | `data/` | Rolling history of snapshots (appended each run). |
| `nl_orgs_dashboard_data.xlsx` | `data/` | Joined dataset consumed by the Streamlit dashboard (`streamlit_app.py`). |
| `comparison_long.csv` / `comparison_pivot.csv` | `data/` | Scenario metrics (affiliation vs. CRIS vs. repository) per organisation. |
| `curated_oai_endpoints.xlsx` | `data/` | Curated OAI endpoint list retrieved from the shared Google Sheet. |
| `img/*.png` | `img/` | PNG charts exported by the notebook (see below). |

All `/data` artifacts are ignored by git to prevent accidental disclosure of credential-derived outputs. The `/img` directory is committed so documentation can embed the latest charts.

---

## Visual Reports

### Organisational coverage

![Total products per organisation](img/org_total_products.png)

### Datasource snapshot (latest)

![Latest datasource totals](img/datasource_totals_latest.png)

### Datasource growth check (latest vs previous)

![Latest vs previous datasource totals](img/datasource_totals_compare.png)

> This comparison becomes meaningful once at least two snapshots exist in the history workbook.

### Organisations vs. their datasources

![Organisation vs data sources](img/org_vs_datasources.png)

### Organisation vs. individual datasources

![Organisation vs each datasource](img/org_vs_datasource_breakdown.png)

### OAI endpoint diagnostics

![Endpoint coverage summary](img/oai_endpoint_summary.png)
![OpenAIRE compatibility by type](img/oai_openaire_compatibility_by_type.png)

### Interactive datasource dashboard (Streamlit)

Run `streamlit run streamlit_app.py` after exporting `data/nl_orgs_dashboard_data.xlsx` to explore the full dashboard outside Jupyter. The app mirrors all notebook-era controls—multi-select filters, range sliders for organisation/datasource totals, “quick focus” presets for common error scenarios, responsive Plotly charts, and a sortable HTML table with OpenAIRE/OAI hyperlinks.

---

## Operational Notes

- **Agents**: Each section of the notebook acts like an “agent” (baseline scout, metrics harvester, datasource cartographer, snapshot scribe, viz painter). See `agents.md` for guidance on invoking just the portion you need—including the dashboard export agent that feeds the Streamlit UI.
- **Dashboard shortcuts**: The Streamlit app includes “Quick focus” presets in the sidebar to immediately highlight zero-product OpenAIRE-compatible datasources or CRIS compatibility mismatches without manually adjusting every filter.
- **Zero-product datasources**: Visual sections list any datasources reporting zero total research products and exclude them from the charts. Use those console messages (or the dashboard preset) to notify repository managers of harvesting gaps.
- **Checkpointing**: Long-running enrichment steps write temporary Excel files (e.g., `nl_orgs_openaire.tmp.xlsx`) every few organisations so work can resume if the notebook is interrupted.

---

## Troubleshooting

| Issue | Remedy |
| --- | --- |
| Missing OpenAIRE credentials | Confirm `.env` is populated and the environment is reloaded before running the notebook. get the credentaisl here [https://develop.openaire.eu/apis] |
| API rate limits | The notebook already authenticates, but if you hit limits reduce the max workers in multithreaded sections or rerun later. |
| History workbook missing | Step 9 creates `nl_orgs_openaire_datasources_numFound_history.xlsx`. If deleted, the next snapshot run recreates it automatically. |
| Comparison chart blank | You must have at least two distinct `date_retrieved` values in the history workbook to generate the side-by-side bars. |

---

## Contributing

Pull requests are welcome. Please:

1. Run the notebook sections relevant to your change so regenerated artifacts stay in sync.
2. Update `agents.md` or this README if you add or rename outputs.
3. Keep sensitive data (credentials, raw tokens, etc.) out of version control—`.gitignore` already excludes `.env` and everything under `/data`.

Questions or suggestions? Open an issue on GitHub and describe which section (agent) of the notebook you are targeting. That keeps discussions well scoped and easier to reproduce.
