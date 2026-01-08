# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo>=0.17.0",
#     "pyzmq>=27.1.0",
# ]
# ///

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Dutch CRIS and Repositories Dashboard
    """)
    return


@app.cell
def _():
    import duckdb
    engine = duckdb.connect("./data/ducklake.duckdb", read_only=True)
    return (engine,)


@app.cell
def _():
    import altair as alt
    import polars as pl
    return alt, pl


@app.cell
def data_combined(engine, mo):
    df_snapshot_datasources_orgs = mo.sql(
        f"""
        SELECT
            o.name AS organisation_name,
            ds.name AS data_source_name,
            ds.type AS data_source_type,
            em.*,
            s.*   
        FROM
            snapshot s
        JOIN
            datasources ds
            ON s.ds_id = ds.ds_id
        JOIN
            endpoint_metrics em
            ON em.ds_id = ds.ds_id
        JOIN
            orgs o
            ON ds.org_id = o.org_id
        ORDER BY
            s.snapshot_date DESC
        """,
        engine=engine
    )
    return (df_snapshot_datasources_orgs,)


@app.cell
def _(alt, df_snapshot_datasources_orgs):
    _chart = (
        alt.Chart(df_snapshot_datasources_orgs)
        .mark_bar()
        .encode(
            x=alt.X(aggregate='count', type='quantitative'),
            y=alt.Y(field='detected_support_oai_cerif_openaire', type='nominal', sort='ascending'),
            color=alt.Color(field='openaire_compatibility', type='nominal'),
            tooltip=[
                alt.Tooltip(field='detected_support_oai_cerif_openaire'),
                alt.Tooltip(aggregate='count'),
                alt.Tooltip(field='openaire_compatibility')
            ]
        )
        .properties(
            title='OpenAIRE Compatibility on CERIF',
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    _chart
    return


@app.cell
def _(alt, df_snapshot_datasources_orgs):
    _chart = (
        alt.Chart(df_snapshot_datasources_orgs)
        .mark_arc(innerRadius=80)
        .encode(
            color=alt.Color(field='data_source_type', type='nominal', scale={
                'scheme': 'dark2'
            }),
            theta=alt.Theta(aggregate='count', type='quantitative'),
            tooltip=[
                alt.Tooltip(aggregate='count'),
                alt.Tooltip(aggregate='count'),
                alt.Tooltip(field='data_source_type')
            ]
        )
        .properties(
            title='Data source Types',
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    _chart
    return


@app.cell
def _(engine):
    engine.close()
    return


@app.cell
def _(pl):
    # ────────────────────────────────────────────────────────
    # Cell 2 – Load a toy dataset (replace with your real one)
    # ────────────────────────────────────────────────────────
    # Example DataFrame – replace this with `df_snapshot_datasources_orgs`
    df = pl.DataFrame(
        {
            "organisation_name": ["Org A"] * 4 + ["Org B"] * 3,
            "data_source_type": ["type1", "type2", "type1", "type3"] * 1 + ["type2", "type3", "type1"],
            "detected_support_oai_cerif_openaire": [True, False, True, False, False, True, False],
            "openaire_compatibility": ["yes", "no", "yes", "no", "no", "yes", "no"],
            "value": [10, 5, 12, 3, 4, 8, 2]
        }
    )
    return (df,)


@app.cell
def _(alt, df, mo):
    # ────────────────────────────────────────────────────────
    # Cell 3 – Prepare the *selection* and a *dropdown* widget
    # ────────────────────────────────────────────────────────
    # We’ll use the “data_source_type” column as the categorical axis.
    # 1️⃣  Create a selection that remembers what the user clicked.
    # 2️⃣  Bind the selection to a dropdown that shows the unique values
    #     – this will be our “underlying‑data” trigger.
    selection = alt.selection_single(
        fields=["data_source_type"],
        bind=alt.binding_select(options=df["data_source_type"].unique().to_list()),
        name="sel"
    )

    # The dropdown itself is just a UI element you can inspect in the UI
    dropdown = mo.ui.dropdown(
        options=df["data_source_type"].unique().to_list(),
        value=df["data_source_type"][0],  # default value
        label="Selected source type"
    )
    dropdown  # display the widget
    return (selection,)


@app.cell
def _(alt, df, selection):
    # ────────────────────────────────────────────────────────
    # Cell 4 – Bar chart that uses the selection
    # ────────────────────────────────────────────────────────
    bar_chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("data_source_type:N", title="Data source type"),
            y=alt.Y("count()", title="Number of rows"),
            color=alt.Color("openaire_compatibility:N", title="OpenAIRE compatibility"),
            tooltip=[
                alt.Tooltip("data_source_type:N", title="Type"),
                alt.Tooltip("count()", title="Count"),
                alt.Tooltip("openaire_compatibility:N", title="Compatibility"),
            ]
        )
        .add_selection(selection)          #  ➜  attaches the click‑selection
        .transform_filter(selection)      #  ➜  keeps only the slice that was clicked
        .properties(
            title="Click a bar to filter the table below",
            width="container",
            height=300,
            config={"view": {"stroke": None}}
        )
    )
    bar_chart
    return


@app.cell
def _(df, mo, pl, selection):
    # ────────────────────────────────────────────────────────
    # Cell 5 – Table that shows the *underlying* rows
    # ────────────────────────────────────────────────────────
    # We react to the value of `selection` (the same field that the dropdown shows).
    # The table updates automatically when the user clicks a new slice.
    filtered_df = df.filter(pl.col("data_source_type") == selection.selected_data_source_type)
    mo.ui.table(filtered_df)
    return


@app.cell
def _(alt, df, selection):
    pie_chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=80)
        .encode(
            theta=alt.Theta("count()", title="Count"),
            color=alt.Color("data_source_type:N", title="Data source type"),
            tooltip=[
                alt.Tooltip("data_source_type:N", title="Type"),
                alt.Tooltip("count()", title="Count")
            ]
        )
        .add_selection(selection)          # attach the same selection
        .transform_filter(selection)      # filter the same way
        .properties(
            title="Click a slice to filter the table below",
            width="container",
            height=300,
            config={"view": {"stroke": None}}
        )
    )
    pie_chart
    return


@app.cell
def _(alt, df, mo, pl):
    selection = alt.selection_single(fields=["data_source_type"], bind=alt.binding_select(options=df["data_source_type"].unique().to_list()))
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="data_source_type:N",
            y="count()",
            color="openaire_compatibility:N",
            tooltip=["data_source_type", "count()", "openaire_compatibility"]
        )
        .add_selection(selection)
        .transform_filter(selection)
    )
    chart  # shows the interactive bar

    # And a table that shows the underlying rows
    mo.ui.table(df.filter(pl.col("data_source_type") == selection.selected_data_source_type))
    return (selection,)


if __name__ == "__main__":
    app.run()
