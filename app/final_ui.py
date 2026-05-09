import os
import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(page_title="Flight Search", layout="wide")

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "flightmgmtsys"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}


@st.cache_resource
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def to_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_query_parts(filters):
    """Build shared WHERE clause and params used by both run_query and run_explain."""
    origin = filters["origin"].strip().upper()
    dest = filters["dest"].strip().upper()
    fl_date = filters["date"].strip()

    sort = filters["sort"]
    limit = min(max(to_int(filters["limit"], 25), 1), 100)

    only_available = filters["only_available"]
    dep_start = filters["dep_start"].strip()
    dep_end = filters["dep_end"].strip()
    dep_delay_max = to_int(filters["dep_delay_max"])

    where_clauses = [
        "origin = %s",
        "dest = %s",
        "fl_date = %s",
    ]
    params = [origin, dest, fl_date]

    if only_available:
        where_clauses.append("cancelled = false")

    if dep_start and dep_end:
        where_clauses.append(
            "LPAD(COALESCE(NULLIF(TRIM(scheduled_dep_time), ''), '0000'), 4, '0') BETWEEN %s AND %s"
        )
        params.extend([dep_start.zfill(4), dep_end.zfill(4)])

    if dep_delay_max is not None:
        where_clauses.append("dep_delay IS NOT NULL AND dep_delay <= %s")
        params.append(dep_delay_max)

    order_map = {
        "Cheapest": "price ASC NULLS LAST",
        "Fastest": "COALESCE(actual_elapsed_minutes, scheduled_elapsed_minutes) ASC NULLS LAST",
        "Departure Time": "LPAD(COALESCE(NULLIF(TRIM(scheduled_dep_time), ''), '0000'), 4, '0') ASC NULLS LAST",
        "Lowest Delay": "COALESCE(dep_delay, 999999) ASC NULLS LAST",
    }
    order_by = order_map.get(sort, order_map["Cheapest"])

    return where_clauses, params, order_by, limit


def run_query(filters):
    where_clauses, params, order_by, limit = build_query_parts(filters)

    sql = f"""
        SELECT
            id, flight_name, fl_date, origin, dest, scheduled_dep_time, scheduled_arr_time,
            scheduled_elapsed_minutes, actual_elapsed_minutes,
            distance, dep_delay, arr_delay, cancelled, price
        FROM flights
        WHERE {" AND ".join(where_clauses)}
        ORDER BY {order_by}, id
        LIMIT %s;
    """
    params.append(limit)

    conn = get_conn()
    return pd.read_sql_query(sql, conn, params=params)


def run_explain(filters):
    """Run EXPLAIN (ANALYZE, BUFFERS) on the same query and return the plan as a string."""
    where_clauses, params, order_by, limit = build_query_parts(filters)

    sql = f"""
        EXPLAIN (ANALYZE, BUFFERS)
        SELECT
            id, flight_name, fl_date, origin, dest, scheduled_dep_time, scheduled_arr_time,
            scheduled_elapsed_minutes, actual_elapsed_minutes,
            distance, dep_delay, arr_delay, cancelled, price
        FROM flights
        WHERE {" AND ".join(where_clauses)}
        ORDER BY {order_by}, id
        LIMIT %s;
    """
    params.append(limit)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return "\n".join(row[0] for row in rows)


def parse_explain_highlights(plan_text):
    highlights = {
        "scan_type": None,
        "execution_time": "—",
        "planning_time": "—",
        "heap_fetches": "N/A",
    }
    for line in plan_text.splitlines():
        s = line.strip()
        if highlights["scan_type"] is None:
            if "Index Only Scan" in s:
                highlights["scan_type"] = "Index Only Scan"
            elif "Index Scan" in s:
                highlights["scan_type"] = "Index Scan"
            elif "Bitmap Heap Scan" in s:
                highlights["scan_type"] = "Bitmap Heap Scan"
            elif "Seq Scan" in s:
                highlights["scan_type"] = "Sequential Scan"
        if "Execution Time:" in s:
            highlights["execution_time"] = s.split("Execution Time:")[-1].strip()
        if "Planning Time:" in s:
            highlights["planning_time"] = s.split("Planning Time:")[-1].strip()
        if "Heap Fetches:" in s:
            highlights["heap_fetches"] = s.split("Heap Fetches:")[-1].strip()
    if highlights["scan_type"] is None:
        highlights["scan_type"] = "Unknown"
    return highlights


# ── Scan type color coding ─────────────────────────────────────────────────
SCAN_COLORS = {
    "Index Only Scan": ("No heap access at all"),
    "Index Scan":      ("Index used, but sometimes heap access involved"),
    "Bitmap Heap Scan":("Bitmap index + heap"),
    "Sequential Scan": ("No index used, postgres performs full table scan"),
}


# ── Page layout ────────────────────────────────────────────────────────────
st.title("Flight Search and Management System")
st.write("Search flights by route, date, departure time, delay, and sort by price or duration.")

with st.sidebar:
    st.header("Search Filters")
    origin = st.text_input("Origin Airport Code", value="JFK")
    dest = st.text_input("Destination Airport Code", value="LAX")
    date = st.date_input("Date")
    sort = st.selectbox("Sort By", ["Cheapest", "Fastest", "Departure Time", "Lowest Delay"])
    limit = st.number_input("Limit", min_value=1, max_value=100, value=25, step=1)
    only_available = st.checkbox("Only non-cancelled flights", value=True)
    dep_start = st.text_input("Departure Start (HHMM)", value="")
    dep_end = st.text_input("Departure End (HHMM)", value="")
    dep_delay_max = st.number_input("Max Departure Delay (mins)", min_value=0, max_value=1000, value=60, step=1)
    search = st.button("Search", use_container_width=True)

if search:
    filters = {
        "origin": origin,
        "dest": dest,
        "date": str(date),
        "sort": sort,
        "limit": limit,
        "only_available": only_available,
        "dep_start": dep_start,
        "dep_end": dep_end,
        "dep_delay_max": dep_delay_max,
    }

    try:
        # ── Run main query ─────────────────────────────────────────────────
        df = run_query(filters)
        st.success(f"**{len(df)}** result(s) returned for **{origin.upper()} → {dest.upper()}** on **{date}**.")
        st.dataframe(df, use_container_width=True, hide_index=True)

        # ── Run EXPLAIN and show results ───────────────────────────────────
        st.divider()
        st.subheader("Query Execution Plan (EXPLAIN ANALYZE)")

        plan_text = run_explain(filters)
        highlights = parse_explain_highlights(plan_text)

        # Summary metric cards
        scan_type = highlights["scan_type"]
        description = SCAN_COLORS.get(scan_type, "")

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Scan Type", f"{scan_type}")
        with col2:
            st.metric("Execution Time", highlights.get("execution_time", "-"))
        with col3:
            st.metric("Planning Time", highlights.get("planning_time", "-"))
        with col4:
            st.metric("Heap Fetches", highlights.get("heap_fetches", "N/A"))

        # Scan type explanation badge
        st.markdown(
           f'<div style="border-left:4px solid #555; padding:8px 14px; border-radius:4px; margin:8px 0;">'
           f'{scan_type} &mdash; {description}</div>',
           unsafe_allow_html=True,
        )

        # Full raw EXPLAIN output
        with st.expander("EXPLAIN ANALYZE Output"):
            st.code(plan_text, language="sql")

    except Exception as e:
        st.error(f"Search failed: {e}")

else:
    st.info("Set your filters in the sidebar and click **Search** to find flights.")
