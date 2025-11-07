# Dutch Sources – OpenAIRE Explorer 🧭

Keeping track of Dutch research activity in the OpenAIRE Graph shouldn’t feel like spelunking through JSON. This notebook-driven toolkit fetches the latest organisational baseline, authenticates with the Graph API, and keeps an auditable history of what every Dutch data source is publishing.

## Why You’ll Like It
- **Zero manual spreadsheets** – every run downloads the official NL baseline and normalises the identifiers for you.
- **Authenticated Graph calls** – client-credential flow handled automatically, so you get the higher rate limits.
- **Built-in observability** – CSV caches, Excel histories, parquet-ready tables, and a rolling `/img` gallery keep evidence handy.
- **Progress bars everywhere** – long API loops run concurrently with clear feedback and checkpoints.

## Visual Tour

### Organisational coverage
![Total products per organisation](img/org_total_products.png)

### Data source snapshot (latest pull)
![Latest totals per data source](img/datasource_totals_latest.png)

### Latest vs. previous snapshot
![Latest vs previous totals per data source](img/datasource_totals_compare.png)

> The comparison chart becomes meaningful once you’ve captured at least two `date_retrieved` snapshots. Until then you’ll see a friendly reminder image.

## How the Notebook Flows
1. **Baseline download** – grabs the published Google Sheet (`nl_orgs_baseline.xlsx`) and keeps the sheet/tab names in sync.
2. **Identifier wrangling** – enriches the baseline with OpenAIRE IDs by resolving ROR links and caching access tokens.
3. **Metric collection** – runs per-organisation scenarios (affiliation vs. CRIS vs. repository) via authenticated Graph calls.
4. **Data source inventory** – fetches every datasource tied to the organisations, along with compatibility metadata.
5. **NumFound snapshots** – records total and per-product counts for each datasource and writes both dated Excel dumps and a cumulative history workbook.
6. **Visualisation & exports** – saves ready-to-share PNGs in `img/` and Excel/CSV/Parquet artifacts in `data/`.

## Repository Layout
```
overview-stats.ipynb   # Main analysis & visualisation notebook
.env.example           # Template for CLIENT_ID / CLIENT_SECRET
data/                  # Generated CSV/Excel artifacts and history logs
img/                   # Auto-saved charts (safe to embed anywhere)
agents.md              # Notes on the automation helpers inside the notebook
```

## Getting Started
```bash
git clone https://github.com/surf-ori/dutch-sources.git
cd dutch-sources
python3 -m venv .venv && source .venv/bin/activate
cp .env.example .env  # fill in CLIENT_ID / CLIENT_SECRET from OpenAIRE AAI
jupyter lab
```
Run `overview-stats.ipynb` top-to-bottom. The first cell installs any missing packages (including `tqdm` and `pyarrow`), and the rest of the notebook assumes the `.env` file is present.

## Key Outputs
| Artifact | Path | What it’s for |
| --- | --- | --- |
| `nl_orgs_baseline.xlsx` | `data/` | the baseline data with all Dutch Research Performing Organisations |
| `nl_orgs_openaire.xlsx` | `data/` | Enriched organisational baseline with OpenAIRE IDs + metrics. |
| `nl_orgs_openaire_datasources.xlsx` | `data/` | All datasources per organisation, including compatibility info. |
| `nl_orgs_openaire_datasources_numFound_YYYY-MM-DD.xlsx` | `data/` | Snapshot of per-datasource totals for that run. |
| `nl_orgs_openaire_datasources_numFound_history.xlsx` | `data/` | Cumulative timeseries (appended every run). |
| `img/*.png` | `img/` | Auto-exported charts for slide decks / reports. |

## Tips & Troubleshooting
- **Need a re-run?** Delete `data/nl_orgs_openaire.xlsx` (or the history workbook) and rerun the targeted steps – the notebook is idempotent.
- **API hiccups?** The progress bars surface failures; simply rerun the cell and the cached access token will be refreshed if necessary.
- **Updating credentials?** Rotate the values in `.env`, restart the kernel, and re-run from the setup cell.
- **Want notebook-free automation?** See `agents.md` for guidance on scripting the enrichment and snapshot steps.

## Share the Love
Issues, ideas, or data quirks? Open a GitHub issue or drop a PR. The more metadata we validate, the easier it becomes to spot stagnant datasources before a harvesting outage hits.
