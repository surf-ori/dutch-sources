# AI Coding Agent Instructions for Dutch Sources

This repository centers on a Jupyter notebook (`overview-stats.ipynb`) that analyzes Dutch university coverage in the OpenAIRE Graph. AI agents should follow these guidelines for effective contributions:

## Project Architecture & Data Flow
- **Single-notebook workflow:** All logic is in `overview-stats.ipynb`. It orchestrates data download, parsing, API authentication, metric collection, and output generation.
- **Data sources:** The notebook fetches a baseline spreadsheet from a public Google Sheet and uses the OpenAIRE Graph API (OAuth client credentials).
- **Outputs:** Results are saved as CSVs (`data/comparison_long.csv`, `data/comparison_pivot.csv`) and visualized with matplotlib.
- **No standalone scripts or modules:** All code is embedded in notebook cells. Helper functions are defined within the notebook.

## Developer Workflow
- **Run in Jupyter:** Launch with `jupyter lab` and execute cells sequentially.
- **Environment setup:**
  - Python 3.10+ recommended.
  - Use a virtual environment (`python3 -m venv .venv; source .venv/bin/activate`).
  - Copy `.env.example` to `.env` and set `CLIENT_ID`/`CLIENT_SECRET` for OpenAIRE API access.
- **Package management:** The notebook installs required packages (`pandas`, `matplotlib`, `openpyxl`, `python-dotenv`, `requests`) in the first cell if missing.
- **Data refresh:** Re-run the notebook to update the baseline table or metrics; the Google Sheet is fetched on each run.

## Patterns & Conventions
- **API access:** Uses OAuth2 client credentials; token is cached and refreshed automatically.
- **Data caching:** Intermediate and final outputs are stored in `data/` (ignored by git).
- **Error handling:** Most errors (API, download) are surfaced via print statements; fatal errors raise exceptions.
- **Identifiers:** University and data source IDs are parsed and normalized from the baseline spreadsheet.
- **Visualization:** Publication counts are compared across scenarios using matplotlib bar charts.

## Integration Points
- **External dependencies:**
  - OpenAIRE Graph API (requires registered client credentials).
  - Google Sheets (public export for baseline data).
- **No custom build/test scripts:** All logic is interactive via notebook cells. No CI/CD or automated tests are present.

## Examples
- To add a new metric, extend the `METRIC_ORDER` and update metric collection logic in the notebook.
- To change the baseline source, update the `nl_orgs_baseline_url` variable in the notebook.
- To troubleshoot API issues, check `.env` values and review print/log output in notebook cells.

## Key Files & Directories
- `overview-stats.ipynb`: Main analysis and all code logic.
- `data/`: Stores generated and cached files (CSV, XLSX).
- `.env.example`: Template for required environment variables.

---
For questions or unclear patterns, review the notebook's markdown cells for rationale and step-by-step guidance. If any conventions or workflows are missing, please request clarification or provide feedback to improve these instructions.
