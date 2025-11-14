from __future__ import annotations

import html
from pathlib import Path
from typing import Sequence

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(page_title="Dutch Sources Monitor", layout="wide")

DATA_PATH = Path("data/nl_orgs_dashboard_data.xlsx")
OPENAIRE_ORG_URL = "https://explore.openaire.eu/search/organization?organizationId="
OPENAIRE_DATASOURCE_URL = "https://explore.openaire.eu/search/dataprovider?datasourceId="
OAI_IDENTIFY_PARAM = "verb=Identify"

VALUE_COLS = [
    "Total Research Products",
    "Publications",
    "Research data",
    "Research software",
    "Other research products",
]

SUPPORT_MAPPING = {
    "OpenAIRE CERIF": "detected_support_oai_cerif_openaire",
    "OpenAIRE Literature": "detected_support_oai_openaire",
    "OpenAIRE Data": "detected_support_openaire_data",
    "NL DIDL": "detected_support_nl_didl",
    "RIOXX": "detected_support_rioxx",
}

SCENARIO_OPTIONS = {
    "None": "none",
    "OpenAIRE compatible datasources with 0 products": "compat_zero",
    "CRIS compatibility mismatch (non-CRIS type)": "cris_mismatch",
}

@st.cache_data(show_spinner=False)
def load_dashboard_data(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Dashboard data file not found: {path}. Rerun the notebook export cell first."
        )
    df = pd.read_excel(path)
    for column in VALUE_COLS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def numeric_range(values: Sequence[float | int]) -> tuple[int, int]:
    if not len(values):
        return (0, 0)
    series = pd.Series(values).dropna()
    if series.empty:
        return (0, 0)
    low = int(series.min())
    high = int(series.max())
    if low == high:
        return (low, low + 1)
    return (low, high)


def format_number(value: float | int) -> str:
    return f"{int(value):,}" if pd.notna(value) else "0"


def append_identify_param(url: str | float | int) -> str:
    if pd.isna(url):
        return ""
    base = str(url).strip()
    if not base:
        return ""
    if OAI_IDENTIFY_PARAM in base:
        return base
    return f"{base}{'&' if '?' in base else '?'}{OAI_IDENTIFY_PARAM}"


def apply_filters(
    df: pd.DataFrame,
    organisations: Sequence[str],
    datasources: Sequence[str],
    compat: Sequence[str],
    types: Sequence[str],
    support: Sequence[str],
    statuses: Sequence[str],
    org_range: tuple[int, int],
    datasource_range: tuple[int, int],
    scenario: str,
) -> pd.DataFrame:
    filtered = df.copy()

    if organisations:
        filtered = filtered[filtered["Organisation Name"].isin(organisations)]
    if compat:
        filtered = filtered[filtered["OpenAIRE Compatibility"].isin(compat)]
    if types:
        filtered = filtered[filtered["Type"].isin(types)]
    if support:
        columns = [SUPPORT_MAPPING[label] for label in support if label in SUPPORT_MAPPING]
        if columns:
            support_mask = filtered[columns].any(axis=1)
            filtered = filtered[support_mask]
    if statuses:
        statuses_lower = {value.lower() for value in statuses}
        filtered = filtered[filtered["oai_status"].str.lower().isin(statuses_lower)]
    if datasources:
        filtered = filtered[filtered["OpenAIRE_DataSource_ID"].isin(datasources)]

    org_min, org_max = org_range
    filtered = filtered[
        filtered["Total Research Products by Affiliation"].fillna(0).between(org_min, org_max)
    ]

    ds_min, ds_max = datasource_range
    filtered = filtered[filtered["Total Research Products"].fillna(0).between(ds_min, ds_max)]

    scenario = scenario.lower().strip()
    if scenario == "compat_zero":
        compat_mask = filtered["OpenAIRE Compatibility"].str.contains("openaire", case=False, na=False)
        zero_mask = filtered["Total Research Products"].fillna(0).eq(0)
        filtered = filtered[compat_mask & zero_mask]
    elif scenario == "cris_mismatch":
        compat_mask = filtered["OpenAIRE Compatibility"].str.contains("cris", case=False, na=False)
        type_mask = ~filtered["Type"].str.contains("cris", case=False, na=False)
        filtered = filtered[compat_mask & type_mask]

    return filtered


def render_metrics(df: pd.DataFrame) -> None:
    org_count = df["OpenAIRE_ORG_ID"].nunique(dropna=True)
    datasource_count = df["OpenAIRE_DataSource_ID"].nunique(dropna=True)
    compat_count = (
        df.loc[df["OpenAIRE Compatibility"].str.lower().ne("not specified"), "OpenAIRE_DataSource_ID"]
        .nunique(dropna=True)
    )
    active_endpoints = (
        df.loc[df["oai_status"].str.lower() == "ok", "OpenAIRE_DataSource_ID"].nunique(dropna=True)
    )
    support_totals = {
        label: int(df[SUPPORT_MAPPING[label]].sum()) for label in SUPPORT_MAPPING
    }
    latest_date = df["Latest Snapshot Date"].dropna().max()
    date_label = pd.to_datetime(latest_date).strftime("%Y-%m-%d") if pd.notna(latest_date) else "Unknown"

    stats = [
        ("Dutch research organisations", format_number(org_count)),
        ("Dutch data sources", format_number(datasource_count)),
        ("OpenAIRE compatible data sources", format_number(compat_count)),
        ("Active OAI endpoints", format_number(active_endpoints)),
        ("NL DIDL supported", format_number(support_totals.get("NL DIDL", 0))),
        ("OpenAIRE CERIF supported", format_number(support_totals.get("OpenAIRE CERIF", 0))),
        ("OpenAIRE literature supported", format_number(support_totals.get("OpenAIRE Literature", 0))),
        ("OpenAIRE data supported", format_number(support_totals.get("OpenAIRE Data", 0))),
        ("Latest snapshot date", date_label),
    ]

    cols = st.columns(3)
    for idx, (label, value) in enumerate(stats):
        with cols[idx % 3]:
            st.metric(label=label, value=value)


def plot_charts(df: pd.DataFrame, latest_date_label: str) -> None:
    charts: list[tuple[str, go.Figure]] = []

    org_chart_df = (
        df.groupby("Organisation Name", as_index=False)["Total Research Products"].sum()
        .rename(columns={"Total Research Products": "Total Research Products by Affiliation"})
        .sort_values("Total Research Products by Affiliation", ascending=False)
    )
    if not org_chart_df.empty:
        fig_org = px.bar(
            org_chart_df,
            x="Total Research Products by Affiliation",
            y="Organisation Name",
            orientation="h",
            title="Research products by affiliated organisation",
        )
        charts.append(("org", fig_org))

    volumetric = df.dropna(subset=VALUE_COLS, how="all").copy()
    if not volumetric.empty:
        fig_products = px.bar(
            volumetric.sort_values("Total Research Products", ascending=False),
            x="Total Research Products",
            y="Datasource Name",
            color="Type",
            orientation="h",
            title=f"Datasource volume snapshot (date: {latest_date_label})",
        )
        charts.append(("datasource", fig_products))

    support_counts = {
        label: df[column].sum() for label, column in SUPPORT_MAPPING.items()
    }
    support_df = pd.DataFrame(
        {"Requirement": list(support_counts.keys()), "Endpoints": list(support_counts.values())}
    )
    fig_support = px.bar(
        support_df,
        x="Requirement",
        y="Endpoints",
        title="Metadata-format support",
    )
    charts.append(("support", fig_support))

    compat_counts = (
        df.groupby("OpenAIRE Compatibility", dropna=False)["OpenAIRE_DataSource_ID"]
        .nunique()
        .reset_index(name="Datasources")
        .sort_values("Datasources", ascending=False)
    )
    if not compat_counts.empty:
        fig_compat = px.bar(
            compat_counts,
            x="Datasources",
            y="OpenAIRE Compatibility",
            orientation="h",
            title="Datasources by OpenAIRE compatibility",
        )
        charts.append(("compat", fig_compat))

    type_counts = (
        df.groupby(["Type", "OpenAIRE Compatibility"], dropna=False)["OpenAIRE_DataSource_ID"]
        .nunique()
        .reset_index(name="Datasources")
    )
    if not type_counts.empty:
        type_pivot = type_counts.pivot(
            index="Type", columns="OpenAIRE Compatibility", values="Datasources"
        ).fillna(0)
        fig_type_heatmap = go.Figure(
            data=[
                go.Heatmap(
                    z=type_pivot.values,
                    x=type_pivot.columns,
                    y=type_pivot.index,
                    colorscale="Blues",
                    colorbar=dict(title="Datasources"),
                )
            ]
        )
        fig_type_heatmap.update_layout(
            title="Datasource types vs. OpenAIRE compatibility",
            xaxis_title="OpenAIRE compatibility",
            yaxis_title="Datasource type",
        )
        charts.append(("type_heatmap", fig_type_heatmap))

    support_records: list[tuple[str, str]] = []
    for label, column in SUPPORT_MAPPING.items():
        mask = df[column]
        if mask.any():
            subset = df.loc[mask, "OpenAIRE Compatibility"].fillna("Not specified")
            support_records.extend([(label, compat) for compat in subset])
    if support_records:
        support_heatmap_df = (
            pd.DataFrame(support_records, columns=["Support", "OpenAIRE Compatibility"])
            .groupby(["Support", "OpenAIRE Compatibility"], dropna=False)
            .size()
            .reset_index(name="Datasources")
        )
        support_pivot = support_heatmap_df.pivot(
            index="Support", columns="OpenAIRE Compatibility", values="Datasources"
        ).fillna(0)
        fig_support_heatmap = go.Figure(
            data=[
                go.Heatmap(
                    z=support_pivot.values,
                    x=support_pivot.columns,
                    y=support_pivot.index,
                    colorscale="Purples",
                    colorbar=dict(title="Datasources"),
                )
            ]
        )
        fig_support_heatmap.update_layout(
            title="Supported endpoint types vs. OpenAIRE compatibility",
            xaxis_title="OpenAIRE compatibility",
            yaxis_title="Endpoint support type",
        )
        charts.append(("support_heatmap", fig_support_heatmap))

    for i in range(0, len(charts), 2):
        cols = st.columns(2)
        for (name, fig), col in zip(charts[i : i + 2], cols):
            col.plotly_chart(fig, use_container_width=True)


def make_org_link(row: pd.Series) -> str:
    org_id = row.get("OpenAIRE_ORG_ID")
    name = html.escape(str(row.get("Organisation Name", "")))
    if pd.isna(org_id) or not str(org_id).strip():
        return name
    escaped_id = html.escape(str(org_id).strip(), quote=True)
    return f"<a href='{OPENAIRE_ORG_URL}{escaped_id}' target='_blank' rel='noopener noreferrer'>{name}</a>"


def make_datasource_link(row: pd.Series) -> str:
    source_id = row.get("OpenAIRE_DataSource_ID")
    name = html.escape(str(row.get("Datasource Name", "")))
    if pd.isna(source_id) or not str(source_id).strip():
        return name
    escaped_id = html.escape(str(source_id).strip(), quote=True)
    return f"<a href='{OPENAIRE_DATASOURCE_URL}{escaped_id}' target='_blank' rel='noopener noreferrer'>{name}</a>"


def make_endpoint_link(url: str | float | int) -> str:
    if pd.isna(url):
        return ""
    base_url = str(url).strip()
    if not base_url:
        return ""
    identify_url = append_identify_param(base_url)
    if not identify_url:
        return html.escape(base_url)
    escaped_base = html.escape(base_url)
    escaped_identify = html.escape(identify_url, quote=True)
    return f"<a href='{escaped_identify}' target='_blank' rel='noopener noreferrer'>{escaped_base}</a>"


def render_table(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("No datasources match the current selection.")
        return

    table_cols = [
        "Organisation Name",
        "Total Research Products by Affiliation",
        "Datasource Name",
        "OpenAIRE Compatibility",
        "Type",
        "OAI-endpoint",
        "oai_status",
        "metadata_prefixes_detected",
        "Total Research Products",
        "Publications",
        "Research data",
        "Research software",
        "Other research products",
    ]
    display_df = df.sort_values(["Organisation Name", "Datasource Name"]).copy()
    display_df["Organisation Name"] = display_df.apply(make_org_link, axis=1)
    display_df["Datasource Name"] = display_df.apply(make_datasource_link, axis=1)
    display_df["OAI-endpoint"] = display_df["OAI-endpoint"].apply(make_endpoint_link)

    st.markdown(
        display_df[table_cols].to_html(index=False, escape=False),
        unsafe_allow_html=True,
    )


def main() -> None:
    st.title("Dutch Sources Monitor")
    st.caption("Insights derived from nl_orgs_dashboard_data.xlsx")

    try:
        dashboard_df = load_dashboard_data(DATA_PATH)
    except FileNotFoundError as exc:
        st.error(str(exc))
        return

    latest_date = dashboard_df["Latest Snapshot Date"].dropna().max()
    latest_label = pd.to_datetime(latest_date).strftime("%Y-%m-%d") if pd.notna(latest_date) else "Unknown"

    st.sidebar.header("Filters")
    org_options = sorted(dashboard_df["Organisation Name"].dropna().unique().tolist())
    compat_options = sorted(dashboard_df["OpenAIRE Compatibility"].dropna().unique().tolist())
    type_options = sorted(dashboard_df["Type"].dropna().unique().tolist())
    support_options = list(SUPPORT_MAPPING.keys())
    status_options = sorted(dashboard_df["oai_status"].dropna().unique().tolist())
    datasource_options = sorted(
        dashboard_df["OpenAIRE_DataSource_ID"].dropna().astype(str).unique().tolist()
    )

    selected_orgs = st.sidebar.multiselect("Organisations", org_options)
    selected_datasources = st.sidebar.multiselect(
        "Datasources (filtered after other selections)", datasource_options
    )
    selected_compat = st.sidebar.multiselect("OpenAIRE compatibility", compat_options)
    selected_types = st.sidebar.multiselect("Datasource types", type_options)
    selected_support = st.sidebar.multiselect("Endpoint support", support_options)
    selected_status = st.sidebar.multiselect("OAI status", status_options)

    org_range = numeric_range(dashboard_df["Total Research Products by Affiliation"].fillna(0))
    datasource_range = numeric_range(dashboard_df["Total Research Products"].fillna(0))
    org_slider = st.sidebar.slider(
        "Total research products by organisation",
        min_value=org_range[0],
        max_value=org_range[1],
        value=org_range,
    )
    datasource_slider = st.sidebar.slider(
        "Total research products by datasource",
        min_value=datasource_range[0],
        max_value=datasource_range[1],
        value=datasource_range,
    )

    scenario_label = st.sidebar.selectbox("Quick focus", list(SCENARIO_OPTIONS.keys()))
    scenario_key = SCENARIO_OPTIONS[scenario_label]

    filtered = apply_filters(
        dashboard_df,
        selected_orgs,
        selected_datasources,
        selected_compat,
        selected_types,
        selected_support,
        selected_status,
        org_slider,
        datasource_slider,
        scenario_key,
    )

    if filtered.empty:
        st.warning("No datasources match the current selection. Adjust filters for more results.")
        return

    render_metrics(filtered)
    st.subheader("Charts")
    plot_charts(filtered, latest_label)
    st.subheader("Detailed table")
    render_table(filtered)


if __name__ == "__main__":
    main()
