# Dutch Sources – Marimo Dashboard & ETL

Monitor how Dutch research organisations and their data sources show up in the OpenAIRE Graph. The repo ships a Marimo dashboard plus a matching ETL pipeline to refresh the underlying data.

- **See it live (recommended):** https://surf-ori.github.io/dutch-sources/
- **Run or edit locally with Marimo:** clone, install deps, and launch the dashboard or ETL as apps/notebooks.

---

## Quick Start (local)

```bash
git clone https://github.com/surf-ori/dutch-sources.git
cd dutch-sources
python -m venv .venv && source .venv/bin/activate
pip install marimo pandas polars duckdb altair openpyxl
```

### 1) Open the dashboard
- As an interactive app: `marimo run overview-stats-dashboard.py`
- As an editable notebook: `marimo edit overview-stats-dashboard.py`

### 2) Refresh the data (ETL)
The dashboard reads the generated files in `data/`. To pull the latest OpenAIRE data:
- As a script: `python overview-stats-etl-pipline.py`
- As a notebook: `marimo edit overview-stats-etl-pipline.py` (or `marimo run ...` to execute headless)

Set your OpenAIRE credentials first:
```bash
cp .env.example .env
# fill CLIENT_ID and CLIENT_SECRET
```

---

## Online dashboard (main CTA)
No install needed—open https://surf-ori.github.io/dutch-sources/ to explore the latest published dashboard.

---

## What each file does
- `overview-stats-dashboard.py` – Marimo app that renders the Dutch CRIS/Repository dashboard (filters, summary cards, tables, charts).
- `overview-stats-etl-pipline.py` – Marimo-based ETL that downloads/refreshes NL organisation baselines, enriches with OpenAIRE IDs & metrics, and writes the dashboard-ready datasets.
- `.env.example` – template for OpenAIRE `CLIENT_ID` and `CLIENT_SECRET`.
- `agents.md` – notes on the automation “agents” inside the ETL.
- `layouts/overview-stats-dashboard.grid.json` – dashboard layout definition used by Marimo.
- `__marimo__/session/*.json` – saved Marimo session state (safe to delete).

---

## Folders at a glance
- `data/` – generated spreadsheets and DuckDB file used by the dashboard (gitignored).
- `img/` – reference screenshots and exported charts.
- `docs/` – static assets for the published GitHub Pages dashboard.
- `ducklake` – bundled DuckDB database (binary).
- `layouts/` – grid layouts for Marimo apps.

---

## Usage notes
- The dashboard loads live Google Sheets for baseline tables; the ETL caches them to `data/`.
- Re-run the ETL before `marimo run overview-stats-dashboard.py` if you need the freshest metrics.
- You can point Marimo at either workflow: `marimo run` to execute, `marimo edit` to tinker with cells UI-style.

---

## Troubleshooting
- **Missing credentials:** ensure `.env` contains `CLIENT_ID` and `CLIENT_SECRET`.
- **Outdated charts:** re-run `overview-stats-etl-pipline.py`, then restart the dashboard.
- **Package issues:** `pip install -r requirements` equivalent is handled by the script header; installing the listed packages above is usually enough.
