# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair==6.0.0",
#     "duckdb==1.4.3",
#     "marimo>=0.17.0",
#     "openpyxl==3.1.5",
#     "pandas==2.3.3",
#     "polars==1.37.1",
#     "pyarrow==22.0.0",
#     "pydantic-ai==1.43.0",
#     "pyzmq>=27.1.0",
#     "sqlglot==28.6.0",
# ]
# ///

import marimo

__generated_with = "0.19.5"
app = marimo.App(
    width="columns",
    layout_file="layouts/overview-stats-dashboard.grid.json",
)

async with app.setup:
    # Initialization code that runs before all other cells
    import marimo as mo
    import micropip

    # Install the packages when running in WASM
    await micropip.install(["polars", "openpyxl"])

    import duckdb
    # engine = duckdb.connect("./data/ducklake.duckdb", read_only=True)
    import altair as alt
    import polars as pl
    import pandas as pd
    import openpyxl


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Dutch CRIS and Repositories Dashboard
    """)
    return


@app.cell
def _():
    mo.md(r"""
    This Dashboard is part of the **PID to Portal project** from SURF and UNL.

    The aim is to have all Dutch Research Organisations have their data sources represented correctly in the [Netherlands Research Portal](https://netherlands.openaire.eu/).
    """)
    return


@app.cell
def _():
    mo.md(r"""
    ![SURFlogo](https://www.surf.nl/themes/surf/logo.svg)
    """)
    return


@app.cell
def _():
    # Get the Curated Baseline table of Research Organisations in NL

    nl_orgs_baseline = pd.read_excel("https://docs.google.com/spreadsheets/d/e/2PACX-1vTDQiWDIaI1SZkPTMNCovicBhA-nQND1drXoUKvrG1O_Ga3hLDRvmQZao_TvNgmNQ/pub?output=xlsx")
    return (nl_orgs_baseline,)


@app.cell
def _():
    # Get the table containing ROR's and OpenAIRE ORG ID's

    orgs_ids_matching = pd.read_excel("https://docs.google.com/spreadsheets/d/e/2PACX-1vTSaXarmKB4RWMlpEDueeMBnwp4_BYJDUwTgBvhqCQ_-hpco9-fa7yZrAIr0T-TIA/pub?output=xlsx")
    return (orgs_ids_matching,)


@app.cell
def _(orgs_ids_matching):
    # Add a column with the URL to the Organisation, pointing to the NL research portal

    orgs_ids_matching_with_links = orgs_ids_matching.assign(
        OpenAIRE_ORG_LINK="https://netherlands.openaire.eu/search/organization?organizationId=" + orgs_ids_matching["OpenAIRE_ORG_ID"]
    )
    return (orgs_ids_matching_with_links,)


@app.cell
def _(nl_orgs_baseline, orgs_ids_matching_with_links):
    # Merge the baseline table continaing RORs, with the table containing ROR's and OpenAIRE ORG ID's

    # SQL join later on was not working in WASM , because the this line produced a polars data frame:
    # SELECT b.full_name_in_English AS name, b.acronym_EN, b.main_grouping AS grouping, m.OpenAIRE_ORG_LINK, m.OpenAIRE_ORG_ID, b.ROR_LINK FROM nl_orgs_baseline b JOIN orgs_ids_matching_with_links m ON b.ROR = m.ROR
    # we replaced it with a merge with Panda's, to keep it Panda's, in order to merge with the data sources data frame later on.

    # keep only the columns you truly need from each side to avoid _x/_y
    left = nl_orgs_baseline[["full_name_in_English", "acronym_EN", "main_grouping", "ROR", "ROR_LINK"]]
    right = orgs_ids_matching_with_links[["ROR", "OpenAIRE_ORG_ID", "OpenAIRE_ORG_LINK"]]

    tmp = left.merge(right, on="ROR", how="inner")

    organisations = tmp.rename(columns={
        "full_name_in_English": "name",
        "main_grouping": "grouping",
    })[["name", "acronym_EN", "grouping", "OpenAIRE_ORG_LINK", "OpenAIRE_ORG_ID", "ROR_LINK"]]
    return (organisations,)


@app.cell
def _():
    # Get the DataSources table

    datasources_baseline = pd.read_excel("https://docs.google.com/spreadsheets/d/e/2PACX-1vQwM24DIUWmqbjxaAy62w9w8gNpOMSg5sxmFro-OexCeMzIlyUJh5iVVsVxyrcLkQ/pub?output=xlsx")
    return (datasources_baseline,)


@app.cell
def _(datasources_baseline):
    # Add a column with the URL to the data source, pointing to the NL research portal

    datasources = datasources_baseline.assign(
        OpenAIRE_DataSource_LINK="https://netherlands.openaire.eu/search/dataprovider?datasourceId=" + datasources_baseline["OpenAIRE_DataSource_ID"]
    )
    return (datasources,)


@app.cell
def _(datasources, organisations):
    orgs_ds = mo.sql(
        f"""
        SELECT * FROM organisations o FULL JOIN datasources d ON o.openaire_org_id = d.openaire_org_id
        """,
        output=False
    )
    return (orgs_ds,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Filters
    """)
    return


@app.cell
def _(orgs_ds):
    # Step 1: Get unique values from the specified columns
    # First, we need to get the unique values from the specified columns in the orgs_ds table. We can do this using the unique method provided by polars.

    unique_grouping = orgs_ds["grouping"].unique()
    unique_names = orgs_ds["name"].unique()
    unique_is_geregistreerd = orgs_ds["is_geregistreerd"].unique()
    unique_in_portal = orgs_ds["in portal"].unique()
    unique_wenselijk = orgs_ds["Wenselijk"].unique()
    unique_akkoord_centraal_nl_beheer = orgs_ds["akkoord centraal NL beheer"].unique()
    unique_type = orgs_ds["Type"].unique()

    # Step 2: Create dropdown widgets
    # Next, we can create dropdown widgets using mo.ui.dropdown. We'll pass the unique values as options to the dropdown.

    grouping_dropdown = mo.ui.dropdown(
        options=["None"] + unique_grouping.to_list(),
        value="None",  # default value
        label=f"{mo.icon('lucide:folder')} Group"
    )

    name_dropdown = mo.ui.dropdown(
        options=["None"] + unique_names.to_list(),
        value="None",  # default value
        label=f"{mo.icon('lucide:landmark')} Org"
    )

    type_dropdown = mo.ui.dropdown(
        options=["None"] + unique_type.to_list(),
        value="None",  # default value
        label=f"{mo.icon('lucide:type')} Type"
    )

    is_geregistreerd_dropdown = mo.ui.dropdown(
        options=["None"] + unique_is_geregistreerd.to_list(),
        value="None",  # default value
        label=f"{mo.icon('lucide:check-square')} Claimed/Registered"
    )

    in_portal_dropdown = mo.ui.dropdown(
        options=["None"] + unique_in_portal.to_list(),
        value="None",  # default value
        label=f"{mo.icon('lucide:globe')} Active In Portal"
    )

    wenselijk_dropdown = mo.ui.dropdown(
        options=["None"] + unique_wenselijk.to_list(),
        value="None",  # default value
        label=f"{mo.icon('lucide:heart')} Required In Portal"
    )

    akkoord_centraal_nl_beheer_dropdown = mo.ui.dropdown(
        options=["None"] + unique_akkoord_centraal_nl_beheer.to_list(),
        value="None",  # default value
        label=f"{mo.icon('lucide:shield')} Managed by SURF"
    )


    # Step 3: Display the dropdown widgets
    # Finally, we can display the dropdown widgets.

    mo.vstack(
        [
            mo.md("### Filters"),

            mo.md("**Organisaties:**"),
            grouping_dropdown, # display the widget
            name_dropdown,  # display the widget

            mo.md("**Data Sources:**"),
            type_dropdown, # display the widget
            is_geregistreerd_dropdown,  # display the widget
            in_portal_dropdown,  # display the widget
            wenselijk_dropdown,  # display the widget
            akkoord_centraal_nl_beheer_dropdown,  # display the widget
        ]
    )


    # If you want to use the selected values for further processing, you can access them using the value attribute of the dropdown widgets, like this:

    # selected_grouping = grouping_dropdown.value
    # selected_name = name_dropdown.value
    # selected_is_geregistreerd = is_geregistreerd_dropdown.value
    # selected_in_portal = in_portal_dropdown.value
    # selected_wenselijk = wenselijk_dropdown.value
    # selected_akkoord_centraal_nl_beheer = akkoord_centraal_nl_beheer_dropdown.value
    return (
        akkoord_centraal_nl_beheer_dropdown,
        grouping_dropdown,
        in_portal_dropdown,
        is_geregistreerd_dropdown,
        name_dropdown,
        type_dropdown,
        wenselijk_dropdown,
    )


@app.cell
def _(
    akkoord_centraal_nl_beheer_dropdown,
    grouping_dropdown,
    in_portal_dropdown,
    is_geregistreerd_dropdown,
    name_dropdown,
    orgs_ds,
    type_dropdown,
    wenselijk_dropdown,
):
    filtered_orgs_ds = orgs_ds

    if grouping_dropdown.value not in (None, "None"):
        filtered_orgs_ds = filtered_orgs_ds.filter(
            pl.col("grouping") == grouping_dropdown.value
        )

    if name_dropdown.value not in (None, "None"):
        filtered_orgs_ds = filtered_orgs_ds.filter(
            pl.col("name") == name_dropdown.value
        )

    if type_dropdown.value not in (None, "None"):
        filtered_orgs_ds = filtered_orgs_ds.filter(
            pl.col("Type") == type_dropdown.value
        )

    if is_geregistreerd_dropdown.value not in (None, "None"):
        filtered_orgs_ds = filtered_orgs_ds.filter(
            pl.col("is_geregistreerd") == is_geregistreerd_dropdown.value
        )

    if in_portal_dropdown.value not in (None, "None"):
        filtered_orgs_ds = filtered_orgs_ds.filter(
            pl.col("in portal") == in_portal_dropdown.value
        )

    if wenselijk_dropdown.value not in (None, "None"):
        filtered_orgs_ds = filtered_orgs_ds.filter(
            pl.col("Wenselijk") == wenselijk_dropdown.value
        )

    if akkoord_centraal_nl_beheer_dropdown.value not in (None, "None"):
        filtered_orgs_ds = filtered_orgs_ds.filter(
            pl.col("akkoord centraal NL beheer") == akkoord_centraal_nl_beheer_dropdown.value
        )

    num_records = filtered_orgs_ds.height
    mo.md(f"Records in selection: {num_records}")
    return (filtered_orgs_ds,)


@app.cell(column=1)
def _():
    mo.md(r"""
    ### Cards
    """)
    return


@app.cell
def _(filtered_orgs_ds):
    mo.stop(filtered_orgs_ds is None)

    # Calculate the required statistics
    total_records = filtered_orgs_ds.height
    ja_is_geregistreerd = filtered_orgs_ds.filter(pl.col("is_geregistreerd") == "Ja").height
    ja_in_portal = filtered_orgs_ds.filter(pl.col("in portal") == "Ja").height
    ja_wenselijk = filtered_orgs_ds.filter(pl.col("Wenselijk") == "Ja").height

    # Create a dictionary to hold the stats
    stats = {
        "# Data Sources": total_records,
        "# Registered in OpenAIRE Graph": ja_is_geregistreerd,
        "# Active in NL Research Portal": ja_in_portal,
        "# Required in NL Research Portal": ja_wenselijk,
    }

    # Create the cards
    _cards = [
        mo.stat(
            label=label.replace("_", " "),
            value=value,
            bordered=True,
        )
        for label, value in stats.items()
    ]

    # Title for the section
    _title = "### **Filtered Data Source Statistics**"

    # Display the cards
    mo.vstack(
        [
            mo.md(_title),
            mo.hstack(_cards, widths="equal", align="center"),
        ]
    )
    return


@app.cell
def _():
    mo.md(r"""
    ### Tables
    """)
    return


@app.cell
def _(filtered_orgs_ds):
    # Select the desired columns and rename them
    table_data = (
        filtered_orgs_ds
        .select([
            pl.col("name").alias("Organisation"),
            pl.col("grouping").alias("Group"),
            pl.col("Name_1").alias("Data source"),
            pl.col("Type").alias("Type"),
            pl.col("is_geregistreerd").alias("is geregistreerd in OpenAIRE Graph"),
            pl.col("in portal").alias("is Zichtbaar in NL Research Portal"),
            pl.col("Wenselijk").alias("is Wenselijk in NL Research Portal"),
            pl.col("OpenAIRE_ORG_LINK").alias("Organisation in NL Research Portal"),
            pl.col("OpenAIRE_DataSource_LINK").alias("DataSource in NL Research Portal"),
            pl.col("websiteUrl").alias("Website")
        ])
    )

    # Display the table
    mo.ui.table(table_data)
    return (table_data,)


@app.cell
def _(table_data):
    # replace _df with your data source
    group_donut_chart = (
        alt.Chart(table_data)
        .mark_arc(innerRadius=50)
        .encode(
            color=alt.Color(field='Group', type='nominal'),
            theta=alt.Theta(aggregate='count', type='quantitative'),
            tooltip=[
                alt.Tooltip(aggregate='count'),
                alt.Tooltip(aggregate='count'),
                alt.Tooltip(field='Group')
            ]
        )
        .properties(
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    group_donut_chart
    return


@app.cell
def _(table_data):
    # replace _df with your data source
    type_donut_chart = (
        alt.Chart(table_data)
        .mark_arc(innerRadius=50)
        .encode(
            color=alt.Color(field='Type', type='nominal'),
            theta=alt.Theta(aggregate='count', type='quantitative'),
            tooltip=[
                alt.Tooltip(aggregate='count'),
                alt.Tooltip(aggregate='count'),
                alt.Tooltip(field='Type')
            ]
        )
        .properties(
            height=290,
            width='container',
            config={
                'axis': {
                    'grid': False
                }
            }
        )
    )
    type_donut_chart
    return


@app.cell
def _():
    mo.md(r"""
    ### Charts
    """)
    return


@app.cell
def _(filtered_orgs_ds):

    # 1. Aggregate in Polars
    datasources_counts = (
        filtered_orgs_ds
        .group_by("Data sources")
        .agg(pl.len().alias("count"))
    )

    # 2. Build the chart from a Pandas DataFrame
    datasources_chart = (
        alt.Chart(datasources_counts.to_pandas())
        .mark_bar()
        .encode(
            x=alt.X("Data sources", type="nominal", title="Data sources"),
            y=alt.Y("count", type="quantitative", title="Number of records"),
            tooltip=[
                alt.Tooltip(
                    "Data sources",
                    type="nominal",
                    title="Data sources",
                ),
                alt.Tooltip(
                    "count",
                    type="quantitative",
                    format=",.0f",
                    title="Number of records",
                ),
            ],
        )
        .properties(width="container",title="Data Sources")
        .configure_view(stroke=None)
    )

    datasources_chart
    return


@app.cell
def _(filtered_orgs_ds):
    # 1. Aggregate and (optionally) take top 10 in Polars
    registered_counts = (
        filtered_orgs_ds
        .group_by("is_geregistreerd")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10)   # for a boolean/low-cardinality field this doesn't really matter, but it's fine
    )

    # 2. Convert to pandas for Altair
    registered_df = registered_counts.to_pandas()

    # 3. Build the chart
    registered_chart = (
        alt.Chart(registered_df)
        .mark_bar()
        .encode(
            y=alt.Y(
                "is_geregistreerd:N",
                sort="-x",
                axis=alt.Axis(title=None),
            ),
            x=alt.X("count:Q", title="Number of records"),
            tooltip=[
                alt.Tooltip("is_geregistreerd:N", title="Geregistreerd"),
                alt.Tooltip("count:Q", format=",.0f", title="Number of records"),
            ],
        )
        .properties(
            width="container",
            title="Aantal Data Sources Geregistreerd in OpenAIRE Graph",
        )
        .configure_view(stroke=None)
        .configure_axis(grid=False)
    )

    registered_chart
    return


@app.cell
def _(filtered_orgs_ds):
    # 1. Aggregate and take top 10 in Polars
    wenselijk_counts = (
        filtered_orgs_ds
        .group_by("Wenselijk")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10)
    )

    # 2. Convert to pandas for Altair
    wenselijk_df = wenselijk_counts.to_pandas()

    # 3. Build the chart
    wenselijk_chart = (
        alt.Chart(wenselijk_df)
        .mark_bar()
        .encode(
            y=alt.Y(
                "Wenselijk:N",
                sort="-x",
                axis=alt.Axis(title=None),
            ),
            x=alt.X("count:Q", title="Number of records"),
            tooltip=[
                alt.Tooltip("Wenselijk:N", title="Wenselijk"),
                alt.Tooltip("count:Q", format=",.0f", title="Number of records"),
            ],
        )
        .properties(width="container",title="Aantal NL Data Sources die Wenselijk zijn om in de NL Research Portal")
        .configure_view(stroke=None)
        .configure_axis(grid=False)
    )

    wenselijk_chart
    return


@app.cell
def _(filtered_orgs_ds):
    # 1. Aggregate and select top 10 in Polars
    is_in_portal_counts = (
        filtered_orgs_ds
        .group_by("in portal")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(10)
    )

    # 2. Convert to pandas for Altair
    is_in_portal_df = is_in_portal_counts.to_pandas()

    # 3. Build the chart
    is_in_portal_chart = (
        alt.Chart(is_in_portal_df)
        .mark_bar()
        .encode(
            y=alt.Y(
                "in portal:N",
                sort="-x",
                axis=alt.Axis(title=None),
            ),
            x=alt.X("count:Q", title="Number of records"),
            tooltip=[
                alt.Tooltip("in portal:N", title="In portal"),
                alt.Tooltip("count:Q", format=",.0f", title="Number of records"),
            ],
        )
        .properties(width="container",title="Number of Data Sources are currently Selected in the NL Research Portal")
        .configure_view(stroke=None)
        .configure_axis(grid=False)
    )

    is_in_portal_chart
    return


@app.cell
def _(filtered_orgs_ds):
    # 1. Prepare data (Polars → Pandas)
    researchoutput_df = (
        filtered_orgs_ds
        .select("Total Research Products")   # only the needed column
        .to_pandas()
    )

    # 2. Build the chart with Altair (using Altair's own binning)
    researchoutput_chart = (
        alt.Chart(researchoutput_df)
        .mark_bar()
        .encode(
            x=alt.X(
                "Total Research Products:Q",
                bin=True,
                title="Total Research Products"
            ),
            y=alt.Y(
                "count()",
                type="quantitative",
                title="Number of records"
            ),
            tooltip=[
                alt.Tooltip(
                    "Total Research Products:Q",
                    bin=True,
                    title="Total Research Products",
                    format=",.2f",
                ),
                alt.Tooltip(
                    "count()",
                    type="quantitative",
                    title="Number of records",
                    format=",.0f",
                ),
            ],
        )
        .properties(width="container",title="Number of Data Sources containing number of Research output records")
        .configure_view(stroke=None)
    )

    researchoutput_chart
    return


@app.cell
def _(filtered_orgs_ds):
    type_counts = (
        filtered_orgs_ds
        .group_by("Type")
        .agg(pl.len().alias("count"))
    )

    type_chart = alt.Chart(type_counts.to_pandas()).mark_bar().encode(
        x=alt.X("Type", title="Data Source Type"),
        y=alt.Y("count", title="Number of Records")
    ).properties(
        title="Number of Records by Data Source Type"
    )

    type_chart.configure_axis(labelFontSize=12, titleFontSize=14)
    return


@app.cell
def _(filtered_orgs_ds):
    grouping_counts = (
        filtered_orgs_ds
        .group_by("grouping")
        .agg(pl.len().alias("count"))
    )

    groupings_chart = alt.Chart(grouping_counts.to_pandas()).mark_bar().encode(
        x=alt.X("grouping", title="Organisation Grouping"),
        y=alt.Y("count", title="Number of Records")
    ).properties(
        title="Number of Records by Organistion Grouping"
    )

    groupings_chart.configure_axis(labelFontSize=12, titleFontSize=14)
    return


if __name__ == "__main__":
    app.run()
