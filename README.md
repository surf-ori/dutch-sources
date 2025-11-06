# Dutch Sources – OpenAIRE Graph Explorer

## TL;DR
- Jupyter notebook that compares Dutch university activity across OpenAIRE Graph scenarios (affiliations, main CRIS, secondary repositories).
- Fetches the latest NL organizations baseline spreadsheet directly from Google Sheets, then calls the OpenAIRE Graph API with client-credential authentication.
- Outputs long and pivot CSV summaries plus visualisations to help spot coverage gaps between sources.

## Project Overview
This repo hosts exploratory tooling for the Dutch Aurora partners to reconcile publication and project counts held in the OpenAIRE Graph.  
The `overview-stats.ipynb` notebook performs the following high-level steps:

1. Download the latest NL organization baseline table (`nl_orgs_baseline.xlsx`) from the shared Google Sheet.
2. Parse identifiers (OpenORG, primary CRIS, secondary repository) for each institution.
3. Obtain an OAuth access token via the OpenAIRE client credentials flow.
4. Collect metrics for each scenario (projects, data sources, research products) using authenticated Graph API requests.
5. Write both long-form and pivot CSV summaries and plot publication comparisons.

Use it to monitor how well institutional sources are represented and to identify missing or stale links between organizations and their data sources.

## Repository Layout
```
overview-stats.ipynb   # Main analysis notebook
requirements.txt       # Python dependencies for running the notebook
.env.example           # Template for OpenAIRE Graph API credentials
```

During execution the notebook creates:
- `nl_orgs_baseline.xslx`: latest baseline table downloaded at run-time.
- `comparison_long.csv` / `comparison_pivot.csv`: cached outputs for downstream use.

## Prerequisites
- Python 3.10+ recommended.
- An OpenAIRE AAI registered client with `CLIENT_ID` and `CLIENT_SECRET`.
- Access to the public Google Sheet containing the NL organizations baseline (download is automatic).

## Getting Started
1. **Clone & enter the repo**
   ```bash
   git clone https://github.com/<your-org>/dutch-sources.git
   cd dutch-sources
   ```
2. **Create a virtual environment (optional but recommended)**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```
4. **Configure OpenAIRE credentials**
   ```bash
   cp .env.example .env
   # edit .env and insert CLIENT_ID / CLIENT_SECRET from the OpenAIRE service portal
   ```
5. **Launch Jupyter**
   ```bash
   jupyter lab
   ```
6. **Run the notebook**
   - Open `overview-stats.ipynb`.
   - Execute cells from top to bottom. The notebook will:
     - Download the latest NL baseline spreadsheet.
     - Load your credentials from `.env`.
     - Request an access token and query the Graph API.
     - Generate CSV outputs and visualisations.

## Environment Variables
The notebook expects these variables to be available (usually via `.env`):

| Variable      | Description                                        |
|---------------|----------------------------------------------------|
| `CLIENT_ID`   | Registered OpenAIRE AAI client identifier.         |
| `CLIENT_SECRET` | Secret associated with the registered client.    |

Keep `.env` out of version control—the default `.gitignore` already excludes it.

## Updating the Baseline Table
`nl_orgs_baseline_url` points to the public Google Sheet export. When the workbook changes, simply re-run the notebook; the `download_nl_orgs_baseline` helper refreshes `nl_orgs_baseline.xslx` before parsing.

## Outputs
- `comparison_long.csv`: tidy format with one row per university, scenario, and metric.
- `comparison_pivot.csv`: pivoted summary ideal for spreadsheet review.
- Matplotlib chart comparing publication counts across the three scenarios.

These can be reused for reporting pipelines or ingested into BI tooling.

## Troubleshooting
- **401/403 errors**: Confirm your OpenAIRE client is authorised for the Graph APIs and the `.env` values are correct.
- **Token expiry**: The notebook caches the token and refreshes it automatically when needed. Restart the kernel if issues persist.
- **Baseline download errors**: Ensure the Google Sheet URL remains published. If network access is blocked, download the file manually and drop it into the repo as `nl_orgs_baseline.xslx`.

## Contributing
Feel free to open issues or pull requests with improvements, extra analyses, or automation ideas. When updating dependencies, please reflect the changes in `requirements.txt` and note any new setup steps here.
