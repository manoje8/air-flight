import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
import os
import time
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Flight Data Dashboard", page_icon="✈️", layout="wide")


@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_ANALYTIC_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

def run_query(query: str) -> pd.DataFrame:
    conn = init_connection()
    print("Snowflake connected!!")
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()


@st.cache_data(ttl=300)
def load_busiest_countries() -> pd.DataFrame:
    return run_query("""
        SELECT WINDOW_START, RANK, ORIGIN_COUNTRY, UNIQUE_FLIGHTS, TOTAL_FLIGHTS
        FROM gold_top_busiest_countries
        WHERE WINDOW_START = (SELECT MAX(WINDOW_START) FROM gold_top_busiest_countries)
        ORDER BY RANK ASC
        LIMIT 20
    """)

@st.cache_data(ttl=300)
def load_velocity_trend() -> pd.DataFrame:
    return run_query("""
        SELECT WINDOW_START, AVG(AVG_VELOCITY) AS GLOBAL_AVG_VELOCITY
        FROM gold_flight_aggregations
        GROUP BY WINDOW_START
        ORDER BY WINDOW_START ASC
    """)

@st.cache_data(ttl=300)
def load_flight_density() -> pd.DataFrame:
    return run_query("""
        SELECT
            LATITUDE_BIN,
            LONGITUDE_BIN,
            SUM(UNIQUE_FLIGHTS)   AS FLIGHTS,
            SUM(TOTAL_POSITIONS)  AS POSITIONS
        FROM gold_flight_density_heatmap
        WHERE WINDOW_START = (SELECT MAX(WINDOW_START) FROM gold_flight_density_heatmap)
        GROUP BY LATITUDE_BIN, LONGITUDE_BIN
    """)

@st.cache_data(ttl=300)
def load_anomalies() -> pd.DataFrame:
    return run_query("""
        SELECT
            WINDOW_START,
            ORIGIN_COUNTRY,
            COUNT(*)                                        AS TOTAL_SAMPLES,
            SUM(CASE WHEN IS_ANOMALY THEN 1 ELSE 0 END)    AS ANOMALY_COUNT,
            AVG(Z_SCORE)                                    AS AVG_Z_SCORE
        FROM gold_speed_anomalies
        WHERE WINDOW_START = (SELECT MAX(WINDOW_START) FROM gold_speed_anomalies)
        GROUP BY WINDOW_START, ORIGIN_COUNTRY
        ORDER BY ANOMALY_COUNT DESC
        LIMIT 20
    """)

@st.cache_data(ttl=300)
def load_daily_summary() -> pd.DataFrame:
    return run_query("""
        SELECT
            DATE,
            ORIGIN_COUNTRY,
            DAILY_FLIGHTS,
            DAILY_AVG_VELOCITY,
            FLIGHTS_7DAY_AVG,
            DOD_CHANGE,
            ON_GROUND_RATIO
        FROM gold_country_daily_summary
        ORDER BY DATE DESC
        LIMIT 500
    """)

@st.cache_data(ttl=300)
def load_altitude_distribution() -> pd.DataFrame:
    return run_query("""
        SELECT
            ORIGIN_COUNTRY,
            ALTITUDE_BIN,
            SUM(UNIQUE_FLIGHTS) AS UNIQUE_FLIGHTS
        FROM gold_altitude_distribution
        WHERE WINDOW_START = (SELECT MAX(WINDOW_START) FROM gold_altitude_distribution)
        GROUP BY ORIGIN_COUNTRY, ALTITUDE_BIN
        ORDER BY ALTITUDE_BIN ASC
    """)


def safe_load(fn):
    try:
        return fn(), None
    except Exception as e:
        return pd.DataFrame(), str(e)


st.title("Real-Time Flight Analytics")
st.markdown("Global flight activity — Snowflake + Airflow medallion pipeline.")

last_refresh = time.strftime("%H:%M:%S")
col_info, col_refresh = st.columns([8, 2])
with col_info:
    st.caption(f"Data cached for 5 min. Last render: {last_refresh}")
with col_refresh:
    if st.button("Refresh"):
        st.cache_data.clear()
        st.rerun()

st.divider()

# --- Fetch all data ---

with st.spinner("Fetching data from Snowflake..."):
    df_countries,        err_countries   = safe_load(load_busiest_countries)
    df_velocity,         err_velocity    = safe_load(load_velocity_trend)
    df_density,          err_density     = safe_load(load_flight_density)
    df_anomalies,        err_anomalies   = safe_load(load_anomalies)
    df_daily,            err_daily       = safe_load(load_daily_summary)
    df_altitude,         err_altitude    = safe_load(load_altitude_distribution)

# Row 1: KPI tiles

st.subheader("Snapshot")
k1, k2, k3, k4 = st.columns(4)

with k1:
    if not df_countries.empty:
        st.metric("Countries tracked", df_countries["ORIGIN_COUNTRY"].nunique())
    else:
        st.metric("Countries tracked", "—")

with k2:
    if not df_countries.empty:
        total = int(df_countries["UNIQUE_FLIGHTS"].sum())
        st.metric("Unique flights (latest window)", f"{total:,}")
    else:
        st.metric("Unique flights (latest window)", "—")

with k3:
    if not df_velocity.empty:
        latest_vel = df_velocity["GLOBAL_AVG_VELOCITY"].iloc[-1]
        prev_vel   = df_velocity["GLOBAL_AVG_VELOCITY"].iloc[-2] if len(df_velocity) > 1 else None
        delta      = round(latest_vel - prev_vel, 1) if prev_vel is not None else None
        st.metric("Avg velocity (km/h)", f"{latest_vel:.1f}", delta=delta)
    else:
        st.metric("Avg velocity (km/h)", "—")

with k4:
    if not df_anomalies.empty:
        total_anomalies = int(df_anomalies["ANOMALY_COUNT"].sum())
        st.metric("Speed anomalies (latest window)", f"{total_anomalies:,}")
    else:
        st.metric("Speed anomalies (latest window)", "—")

st.divider()

# Row 2: Busiest countries + velocity trend

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 20 busiest countries")
    if err_countries:
        st.warning(f"Could not load country data: {err_countries}")
    elif df_countries.empty:
        st.info("No data available.")
    else:
        fig_bar = px.bar(
            df_countries.sort_values("UNIQUE_FLIGHTS", ascending=True),
            x="UNIQUE_FLIGHTS",
            y="ORIGIN_COUNTRY",
            orientation="h",
            color="UNIQUE_FLIGHTS",
            color_continuous_scale="Blues",
            labels={"UNIQUE_FLIGHTS": "Unique flights", "ORIGIN_COUNTRY": "Country"},
        )
        fig_bar.update_layout(
            coloraxis_showscale=False,
            margin=dict(l=0, r=0, t=24, b=0),
            height=420,
        )
        st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    st.subheader("Global average velocity trend")
    if err_velocity:
        st.warning(f"Could not load velocity data: {err_velocity}")
    elif df_velocity.empty:
        st.info("No data available.")
    else:
        fig_line = px.line(
            df_velocity,
            x="WINDOW_START",
            y="GLOBAL_AVG_VELOCITY",
            markers=True,
            labels={"WINDOW_START": "Window", "GLOBAL_AVG_VELOCITY": "Avg velocity (km/h)"},
        )
        fig_line.update_layout(margin=dict(l=0, r=0, t=24, b=0), height=420)
        st.plotly_chart(fig_line, use_container_width=True)

st.divider()

# Row 3: World map

st.subheader("Flight density heatmap")
if err_density:
    st.warning(f"Could not load density data: {err_density}")
elif df_density.empty:
    st.info("No data available.")
else:
    fig_map = px.density_mapbox(
        df_density,
        lat="LATITUDE_BIN",
        lon="LONGITUDE_BIN",
        z="FLIGHTS",
        radius=10,
        center=dict(lat=20, lon=0),
        zoom=1,
        mapbox_style="carto-darkmatter",
        color_continuous_scale="Inferno",
        labels={"FLIGHTS": "Unique flights"},
    )
    fig_map.update_layout(margin=dict(r=0, t=0, l=0, b=0), height=500)
    st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# Row 4: Anomalies + altitude distribution

col3, col4 = st.columns(2)

with col3:
    st.subheader("Speed anomalies by country")
    if err_anomalies:
        st.warning(f"Could not load anomaly data: {err_anomalies}")
    elif df_anomalies.empty:
        st.info("No anomaly data available.")
    else:
        fig_anom = px.bar(
            df_anomalies.sort_values("ANOMALY_COUNT", ascending=True),
            x="ANOMALY_COUNT",
            y="ORIGIN_COUNTRY",
            orientation="h",
            color="AVG_Z_SCORE",
            color_continuous_scale="Reds",
            labels={
                "ANOMALY_COUNT": "Anomalous flights",
                "ORIGIN_COUNTRY": "Country",
                "AVG_Z_SCORE": "Avg Z-score",
            },
        )
        fig_anom.update_layout(margin=dict(l=0, r=0, t=24, b=0), height=380)
        st.plotly_chart(fig_anom, use_container_width=True)

with col4:
    st.subheader("Altitude distribution (top 5 countries)")
    if err_altitude:
        st.warning(f"Could not load altitude data: {err_altitude}")
    elif df_altitude.empty:
        st.info("No altitude data available.")
    else:
        top_countries = (
            df_altitude.groupby("ORIGIN_COUNTRY")["UNIQUE_FLIGHTS"]
            .sum()
            .nlargest(5)
            .index.tolist()
        )
        df_alt_filtered = df_altitude[df_altitude["ORIGIN_COUNTRY"].isin(top_countries)]
        fig_alt = px.line(
            df_alt_filtered,
            x="ALTITUDE_BIN",
            y="UNIQUE_FLIGHTS",
            color="ORIGIN_COUNTRY",
            markers=True,
            labels={
                "ALTITUDE_BIN": "Altitude bin (m)",
                "UNIQUE_FLIGHTS": "Unique flights",
                "ORIGIN_COUNTRY": "Country",
            },
        )
        fig_alt.update_layout(margin=dict(l=0, r=0, t=24, b=0), height=380)
        st.plotly_chart(fig_alt, use_container_width=True)

st.divider()

# Row 5: Daily summary explorer

st.subheader("Daily country summary")
if err_daily:
    st.warning(f"Could not load daily summary: {err_daily}")
elif df_daily.empty:
    st.info("No daily summary data available.")
else:
    available_countries = sorted(df_daily["ORIGIN_COUNTRY"].dropna().unique())
    selected_countries = st.multiselect(
        "Filter by country",
        options=available_countries,
        default=available_countries[:5],
    )

    df_daily_filtered = df_daily[df_daily["ORIGIN_COUNTRY"].isin(selected_countries)]

    tab1, tab2, tab3 = st.tabs(["Daily flights", "Avg velocity", "Ground ratio"])

    with tab1:
        fig_daily = px.line(
            df_daily_filtered,
            x="DATE",
            y="DAILY_FLIGHTS",
            color="ORIGIN_COUNTRY",
            line_group="ORIGIN_COUNTRY",
            labels={"DATE": "Date", "DAILY_FLIGHTS": "Daily flights"},
        )
        # Overlay 7-day average
        for country in selected_countries:
            df_c = df_daily_filtered[df_daily_filtered["ORIGIN_COUNTRY"] == country]
            fig_daily.add_trace(go.Scatter(
                x=df_c["DATE"],
                y=df_c["FLIGHTS_7DAY_AVG"],
                mode="lines",
                line=dict(dash="dot", width=1),
                name=f"{country} (7d avg)",
                showlegend=True,
            ))
        fig_daily.update_layout(margin=dict(l=0, r=0, t=24, b=0), height=380)
        st.plotly_chart(fig_daily, use_container_width=True)

    with tab2:
        fig_vel = px.line(
            df_daily_filtered,
            x="DATE",
            y="DAILY_AVG_VELOCITY",
            color="ORIGIN_COUNTRY",
            labels={"DATE": "Date", "DAILY_AVG_VELOCITY": "Avg velocity (km/h)"},
        )
        fig_vel.update_layout(margin=dict(l=0, r=0, t=24, b=0), height=380)
        st.plotly_chart(fig_vel, use_container_width=True)

    with tab3:
        fig_gnd = px.area(
            df_daily_filtered,
            x="DATE",
            y="ON_GROUND_RATIO",
            color="ORIGIN_COUNTRY",
            labels={"DATE": "Date", "ON_GROUND_RATIO": "On-ground ratio"},
        )
        fig_gnd.update_layout(margin=dict(l=0, r=0, t=24, b=0), height=380)
        st.plotly_chart(fig_gnd, use_container_width=True)
