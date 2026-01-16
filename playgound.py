import marimo

__generated_with = "0.18.1"
app = marimo.App(width="full", app_title="ETL Playground")


@app.cell
def _():
    import marimo as mo
    import duckdb
    from pathlib import Path
    import os
    import requests
    import signal
    return Path, duckdb, mo, requests


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH '{_ducklake_path}' AS ducklake;
        """
    )
    return


@app.cell
def _():
    return


@app.cell
def _(mo):
    _df = mo.sql(
        f"""
        ATTACH 'ducklake:https://explore.openresearchinf.src.surf-hosted.nl/sprouts.ducklake' AS sprouts;
        """
    )
    return


@app.cell
def _(engine, mo):
    # Describe the schema of a specific table in the sprouts database
    table_schema = mo.sql(
        """
        DESCRIBE sprouts.example_table;
        """,
        engine=engine
    )
    table_schema

    # Query some data from the specific table
    sample_data = mo.sql(
        """
        SELECT * FROM sprouts.example_table LIMIT 10;
        """,
        engine=engine
    )
    sample_data
    return


@app.cell
def _(Path, requests):
    # Download the data dump TAR file

    url = "https://zenodo.org/records/10521976/files/heritage-science.tar?download=1"
    out_dir = Path("./data/playgound")
    out_dir.mkdir(parents=True, exist_ok=True)
    downloaded_file_path = out_dir / "heritage-science.tar"

    if not downloaded_file_path.is_file():
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(downloaded_file_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

    downloaded_file_path
    return downloaded_file_path, out_dir


@app.cell
def _(downloaded_file_path, out_dir):
    # Extract the TAR file

    import tarfile

    # Create a directory named after the tar file (without extension)
    extraction_path = out_dir
    extraction_path.mkdir(parents=True, exist_ok=True)

    # Extract all contents into that directory
    with tarfile.open(downloaded_file_path, mode="r") as tar:
        tar.extractall(path=extraction_path)

    extraction_path
    return


@app.cell
def _(downloaded_file_path, out_dir):
    # Set Extraction Path variables

    extraction_folder_path = out_dir / downloaded_file_path.stem
    first_gz_path = next(extraction_folder_path.rglob("*.gz"))
    first_gz_path
    return (first_gz_path,)


@app.cell
def _(duckdb, out_dir):
    # Close any existing DuckDB connection
    if "engine" in globals():
        try:
            engine.close()
            print("✅ Closed previous DuckDB connection.")
        except Exception as e:
            print(f"⚠️ Error closing previous engine: {e}")

    # Define the path for the new DuckDB file inside the playground folder
    _ducklake_path = out_dir / "ducklake"

    # Check for an existing lock file and remove it if safe
    _lock_path = _ducklake_path.with_suffix(".lock")
    if _lock_path.is_file():
        try:
            _pid = int(_lock_path.read_text().strip())
            print(f"🔎 Existing lock file → PID {_pid}")
        except Exception:
            print("⚠️ Could not read PID from lock file.")
        try:
            _lock_path.unlink()
            print("🔓 Removed stale lock file.")
        except Exception as e:
            print(f"❌ Failed to remove lock file: {e}")

    # Connect (or create) the DuckDB database at the specified location
    try:
        engine = duckdb.connect(str(_ducklake_path), read_only=False)
        print(f"✅ Connected to DuckDB at '{_ducklake_path}'.")
    except Exception as e:
        print(f"🚫 Could not connect to '{_ducklake_path}': {e}")
        engine = duckdb.connect(database=":memory:", read_only=False)
        print("⚙️ Connected to an in‑memory DuckDB instead.")

    engine
    return (engine,)


@app.cell
def _(engine, first_gz_path, mo):
    sql_schema_df = mo.sql(
        f"""
        -- Show the Metadata Schema

        DESCRIBE

        FROM read_json('{first_gz_path}')
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
