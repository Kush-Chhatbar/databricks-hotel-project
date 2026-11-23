"""Hotel Bookings dashboard — Databricks App (Streamlit).

Queries the gold-layer tables produced by the DLT_GOLD_LAYER pipeline:
  workspace.gold.gold_hotel_booking_summary
  workspace.gold.gold_monthly_booking_trends
  workspace.gold.gold_monthly_room_revenue

Authentication uses the app's service principal, injected automatically by
Databricks Apps as DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET.
"""
import os

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "f7fbce46a7a51d65")

st.set_page_config(page_title="Hotel Bookings", page_icon="🏨", layout="wide")
st.title("🏨 Hotel Bookings Dashboard")


@st.cache_resource
def get_client():
    return WorkspaceClient()


@st.cache_data(ttl=300)
def run_query(sql_text: str) -> pd.DataFrame:
    w = get_client()
    response = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql_text,
        wait_timeout="50s",
        format=Format.JSON_ARRAY,
        disposition=Disposition.INLINE,
    )
    if response.status.state != StatementState.SUCCEEDED:
        err = response.status.error.message if response.status.error else response.status.state
        raise RuntimeError(f"Query failed: {err}")

    # In this SDK version, the schema lives on response.manifest and rows are plain lists.
    columns = [c.name for c in response.manifest.schema.columns]
    rows = list(response.result.data_array or [])
    next_chunk = response.result.next_chunk_index
    while next_chunk is not None:
        chunk = get_client().statement_execution.get_statement_result_chunk_n(
            statement_id=response.statement_id,
            chunk_index=next_chunk,
        )
        rows.extend(chunk.data_array or [])
        next_chunk = chunk.next_chunk_index

    return pd.DataFrame(rows, columns=columns)


@st.cache_data(ttl=300)
def load_data():
    summary = run_query("SELECT * FROM workspace.gold.gold_hotel_booking_summary")
    trends = run_query(
        "SELECT * FROM workspace.gold.gold_monthly_booking_trends ORDER BY arrival_year, "
        "array_position(array('January','February','March','April','May','June','July',"
        "'August','September','October','November','December'), arrival_month)"
    )
    revenue = run_query(
        "SELECT * FROM workspace.gold.gold_monthly_room_revenue ORDER BY arrival_year, "
        "array_position(array('January','February','March','April','May','June','July',"
        "'August','September','October','November','December'), arrival_month)"
    )
    # Values arrive as strings from the JSON_ARRAY format — coerce numeric columns.
    for col in ["total_bookings", "canceled_bookings", "non_canceled_bookings"]:
        summary[col] = pd.to_numeric(summary[col], errors="coerce").fillna(0).astype(int)
    for col in ["total_bookings", "canceled_bookings"]:
        trends[col] = pd.to_numeric(trends[col], errors="coerce").fillna(0).astype(int)
    for col in ["estimated_room_revenue", "checked_out_bookings", "average_adr"]:
        revenue[col] = pd.to_numeric(revenue[col], errors="coerce")
    return summary, trends, revenue


try:
    summary, trends, revenue = load_data()
except Exception as e:  # noqa: BLE001 - surface any failure in the UI
    st.error(f"Failed to load gold tables: {e}")
    st.stop()

hotels = sorted(summary["hotel"].dropna().unique().tolist())
selected = st.multiselect("Hotels", hotels, default=hotels)
filtered = summary[summary["hotel"].isin(selected)]

if filtered.empty:
    st.info("No data for the selected hotels.")
    st.stop()

total = int(filtered["total_bookings"].sum())
canceled = int(filtered["canceled_bookings"].sum())
non_canceled = int(filtered["non_canceled_bookings"].sum())
cancel_rate = (canceled / total * 100) if total else 0

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total bookings", f"{total:,}")
c2.metric("Canceled", f"{canceled:,}")
c3.metric("Completed", f"{non_canceled:,}")
c4.metric("Cancellation rate", f"{cancel_rate:.1f}%")

left, right = st.columns(2)

with left:
    st.subheader("Bookings by hotel")
    by_hotel = (
        filtered[["hotel", "total_bookings", "canceled_bookings"]]
        .set_index("hotel")
        .sort_values("total_bookings", ascending=False)
    )
    st.bar_chart(by_hotel)

with right:
    st.subheader("Monthly booking trends")
    ft = trends[trends["hotel"].isin(selected)]
    ft = ft.assign(
        month_label=ft["arrival_year"].astype(str) + " " + ft["arrival_month"].str[:3]
    )
    pivot = ft.pivot_table(
        index="month_label",
        columns="hotel",
        values="total_bookings",
        aggfunc="sum",
    )
    st.line_chart(pivot)

st.subheader("Monthly room revenue (checked-out stays)")
fr = revenue[revenue["hotel"].isin(selected)]
st.dataframe(fr, use_container_width=True, hide_index=True)
