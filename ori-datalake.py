# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb==1.4.4",
#     "marimo",
#     "pydantic-ai==1.58.0",
#     "numpy==2.4.2",  # required for DuckDB -> pandas dataframes
#     "pandas==3.0.0",  # required for DuckDB .df() materialization
#     "altair==6.0.0",
#     "sqlglot==28.10.1",
#     "openlayers==0.1.6",
#     "pyarrow==23.0.0",
#     "nbconvert==7.17.0",
#     "nbformat==5.10.4",
#     "playwright==1.58.0",
#     "pandoc==2.4",
# ]
# ///

import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # ORI Datalake Exploration
    """)
    return


@app.cell
def _():
    import marimo as mo
    import duckdb
    import numpy as np
    import pandas as pd
    import altair as alt
    import openlayers as ol
    import json
    import urllib.parse
    import urllib.request
    import pyarrow

    return alt, duckdb, json, mo, ol, pd, urllib


@app.cell
def _(duckdb, mo):
    ### Attach the remote Duck Lake database and return a reusable connection.
    DUCKLAKE_URL = (
        "ducklake:"
        "https://objectstore.surf.nl/cea01a7216d64348b7e51e5f3fc1901d:"
        "boto3bucket/sprouts_http.ducklake"
    )

    engine = duckdb.connect()
    engine.execute(f"ATTACH '{DUCKLAKE_URL}' AS sprouts;")
    engine.execute("USE sprouts;")

    mo.md(
        "Connected to Duck Lake as `sprouts`. Metadata is available under "
        "`__ducklake_metadata_sprouts`.")
    return (engine,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Inventory of available schemas and tables in the attached catalog.
    """)
    return


@app.cell
def _(engine, mo):
    tables_df = mo.sql(
        f"""
        SELECT s.schema_name, t.table_name
        FROM __ducklake_metadata_sprouts.ducklake_schema s
        JOIN __ducklake_metadata_sprouts.ducklake_table t USING (schema_id)
        WHERE s.schema_name != 'main'
        ORDER BY s.schema_name, t.table_name
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Quick peek at the OpenAIRE publications table.
    """)
    return


@app.cell
def _(engine, mo):
    sample_publications = mo.sql(
        f"""
        SELECT publicationDate, mainTitle
        FROM openaire.publications
        LIMIT 10
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(engine, mo):
    sample_random_publications = mo.sql(
        f"""
        SELECT *
        FROM openaire.publications
        LIMIT 10
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Lightweight summaries to understand the dataset shape.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Total Number of Records in Publications Table
    """)
    return


@app.cell(hide_code=True)
def _(engine, mo):
    df_record_count = mo.sql(
        f"""
        SELECT count(*) AS total_records
        FROM openaire.publications
        """,
        engine=engine
    )
    return (df_record_count,)


@app.cell
def _(df_record_count, mo):
    mo.md(f"""
    The total number of records in the `publications` table is **{df_record_count.total_records.iloc[0]}**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Number of records per publication year
    """)
    return


@app.cell
def _(engine, mo):
    pubs_by_year = mo.sql(
        f"""
        SELECT EXTRACT(YEAR FROM publicationDate) AS year, count(*) AS publications
        FROM openaire.publications
        WHERE publicationDate IS NOT NULL
        GROUP BY year
        ORDER BY year DESC
        LIMIT 20
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Number of Records by Open Access Status
    """)
    return


@app.cell
def _(engine, mo):
    access_breakdown = mo.sql(
        f"""
        SELECT
            COALESCE(bestAccessRight.label, 'unknown') AS access_rights,
            count(*) AS publications
        FROM openaire.publications
        GROUP BY access_rights
        ORDER BY publications DESC
        LIMIT 10
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Number of Records with Country NL
    """)
    return


@app.cell(hide_code=True)
def _(engine, mo):
    df_nl_records = mo.sql(
        f"""
        SELECT count(*) AS nl_records
        FROM openaire.publications p
        WHERE EXISTS (
            SELECT 1
            FROM UNNEST(p.countries) AS u(country)
            WHERE COALESCE(
                CASE WHEN typeof(country) = 'STRUCT' THEN country.code END,
                CASE WHEN typeof(country) = 'VARCHAR' THEN country::VARCHAR END
            ) = 'NL'
        )
        """,
        engine=engine
    )
    return (df_nl_records,)


@app.cell
def _(df_nl_records, mo):
    mo.md(f"""
    The number of records in the `publications` table where the country is NL is **{df_nl_records.nl_records.iloc[0]}**.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Number of Records for Each Country
    """)
    return


@app.cell(hide_code=True)
def _(engine, mo):
    df_country_counts = mo.sql(
        f"""
        SELECT
            COALESCE(
                CASE WHEN typeof(country) = 'STRUCT' THEN country.code END,
                CASE WHEN typeof(country) = 'VARCHAR' THEN country::VARCHAR END
            ) AS country_code,
            COUNT(*) AS record_count
        FROM openaire.publications p,
        UNNEST(p.countries) AS u(country)
        GROUP BY country_code
        ORDER BY record_count DESC
        """,
        engine=engine
    )
    return (df_country_counts,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### country donut chart
    """)
    return


@app.cell
def _(alt, df_country_counts):
    country_chart = (
        alt.Chart(df_country_counts)
        .mark_arc(innerRadius=70)
        .encode(
            color=alt.Color(field='country_code', type='nominal'),
            theta=alt.Theta(field='record_count', type='quantitative'),
            tooltip=[
                alt.Tooltip(field='record_count'),
                alt.Tooltip(field='record_count', format=',.0f'),
                alt.Tooltip(field='country_code')
            ]
        )
        .properties(
            title='Records per Country',
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    country_chart
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    #### Country Geo Heatmap
    """)
    return


@app.cell
def _(df_country_counts, json, mo, ol, pd, urllib):
    # Prepairing the data: a GeoJSON FeatureCollection of country centroids weighted by publication counts.
    centroid_url = (
        "https://raw.githubusercontent.com/360-info/country-centroids/refs/heads/main/data/centroids.csv"
    )

    centroids = pd.read_csv(centroid_url)
    cols = {c.lower(): c for c in centroids.columns}
    key_cols = [k for k in ["iso2", "iso_2", "alpha2", "iso3", "iso_3", "alpha3"] if k in cols]

    iso_to_point = {}
    for _, row in centroids.iterrows():
        lon = row.get(cols.get("lon")) if "lon" in cols else None
        lat = row.get(cols.get("lat")) if "lat" in cols else None
        if pd.isna(lon) or pd.isna(lat):
            continue
        try:
            lon_f = float(lon)
            lat_f = float(lat)
        except Exception:
            continue
        for k in key_cols:
            val = row.get(cols[k])
            if pd.isna(val):
                continue
            code = str(val).strip().upper()
            if code:
                iso_to_point[code] = (lon_f, lat_f)

    features = []
    for _, row in df_country_counts.iterrows():
        code = str(row.country_code).upper()
        weight = int(row.record_count)
        coords = iso_to_point.get(code)
        if not coords:
            continue
        lon, lat = coords
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {"weight": weight, "code": code},
            }
        )

    geojson = {"type": "FeatureCollection", "features": features}
    data_url = "data:application/json," + urllib.parse.quote(json.dumps(geojson))

    # Defining the Geo chart
    radius = 12
    blur = 22

    vector = ol.VectorLayer(source=ol.VectorSource(url=data_url))
    heatmap = ol.HeatmapLayer(
        source=ol.VectorSource(url=data_url),
        opacity=0.55,
        weight=["get", "weight"],
        radius=radius,
        blur=blur,
    )

    m = ol.MapWidget(layers=[ol.BasemapLayer(), vector, heatmap])
    m.add_tooltip()
    m

    # Defining sliders
    radius_slider = mo.ui.slider(
        start=4,
        stop=40,
        step=1,
        value=radius,
        label="radius",
        on_change=lambda r: m.add_layer_call(heatmap.id, "setRadius", r),
    )

    blur_slider = mo.ui.slider(
        start=4,
        stop=60,
        step=1,
        value=blur,
        label="blur",
        on_change=lambda b: m.add_layer_call(heatmap.id, "setBlur", b),
    )


    # show in a Verticle stack
    mo.vstack(
        [mo.md("### Heatmap: publications by country"),
        m,
        radius_slider,
        blur_slider]
    )
    return


@app.cell
def _(engine, mo):
    geolocations = mo.sql(
        f"""
        SELECT "geoLocations" FROM "openaire"."publications" LIMIT 100
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    authors_df = mo.sql(
        f"""
        SELECT id, unnest(authors) FROM openaire.publications LIMIT 10
        """,
        engine=engine
    )
    return


@app.cell
def _(engine, mo):
    authors_unpacked_df = mo.sql(
        f"""
        CREATE OR REPLACE VIEW memory.unpacked AS SELECT id, unnest(authors, recursive := true) FROM sprouts.openaire.publications LIMIT 10;
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
