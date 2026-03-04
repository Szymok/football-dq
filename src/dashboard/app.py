"""
Football DQ Dashboard – Streamlit App

Uruchomienie:
    streamlit run src/dashboard/app.py
"""

import sys
import os

# Dodaj root projektu do PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sqlite3

from src.config.settings import DB_PATH

# ─── Page Config ───────────────────────────────────────────────

st.set_page_config(
    page_title="Football DQ Monitor",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ────────────────────────────────────────────────

st.markdown("""
<style>
    /* Main background */
    .stApp {
        background: linear-gradient(135deg, #0a0f1c 0%, #1a1f2e 50%, #0d1117 100%);
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #161b22 0%, #0d1117 100%);
        border-right: 1px solid #30363d;
    }

    /* Cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #161b22 0%, #1c2333 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }

    div[data-testid="stMetric"] label {
        color: #8b949e !important;
        font-size: 0.85rem;
    }

    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #f0f6fc !important;
        font-weight: 700;
    }

    /* Headers */
    h1, h2, h3 {
        color: #f0f6fc !important;
    }

    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        background-color: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px 8px 0 0;
        color: #8b949e;
        padding: 8px 16px;
    }

    .stTabs [aria-selected="true"] {
        background-color: #1f6feb !important;
        color: #fff !important;
        border-color: #1f6feb;
    }

    /* Dataframes */
    .stDataFrame {
        border: 1px solid #30363d;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)


# ─── DB Connection ─────────────────────────────────────────────

@st.cache_resource
def get_connection():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data(ttl=60)
def query_df(sql: str) -> pd.DataFrame:
    conn = get_connection()
    return pd.read_sql_query(sql, conn)


# ─── Sidebar ───────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⚽ Football DQ")
    st.markdown("**Data Quality Monitor**")
    st.markdown("---")

    page = st.radio(
        "Nawigacja",
        ["📊 DQ Scorecard", "👥 Player Stats", "🏟️ Matches", "🔍 DQ Details"],
        index=0,
    )

    st.markdown("---")
    st.markdown("##### Źródła danych")
    st.markdown("- FBref (via soccerdata)")
    st.markdown("- Understat (via soccerdata)")

    st.markdown("---")
    st.caption(f"Baza: {DB_PATH.name}")
    try:
        total = query_df("SELECT count(*) as cnt FROM player_match_stats").iloc[0]["cnt"]
        st.caption(f"Rekordów: {total}")
    except Exception:
        st.caption("Brak danych – uruchom load_data.py")


# ─── PAGE: DQ Scorecard ───────────────────────────────────────

def page_scorecard():
    st.title("📊 Data Quality Scorecard")

    try:
        dq_df = query_df("""
            SELECT check_name, dimension, passed, value, threshold, details, run_at
            FROM dq_check_results
            ORDER BY run_at DESC
        """)
    except Exception:
        st.error("Brak wyników DQ. Uruchom najpierw: `python scripts/run_dq.py`")
        return

    if dq_df.empty:
        st.warning("Brak wyników DQ w bazie.")
        return

    # Latest run
    latest_run = dq_df["run_at"].max()
    latest = dq_df[dq_df["run_at"] == latest_run]

    total_checks = len(latest)
    passed = latest["passed"].sum()
    failed = total_checks - passed
    overall_pct = round((passed / total_checks) * 100, 1) if total_checks else 0

    # KPI Row
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Overall DQ Score", f"{overall_pct}%")
    with col2:
        st.metric("Total Checks", total_checks)
    with col3:
        st.metric("Passed ✅", int(passed))
    with col4:
        st.metric("Failed ❌", int(failed))

    st.markdown("---")

    # Dimension scores
    st.subheader("Wyniki per wymiar")

    dim_scores = latest.groupby("dimension").agg(
        total=("passed", "count"),
        passed=("passed", "sum"),
    ).reset_index()
    dim_scores["score"] = round((dim_scores["passed"] / dim_scores["total"]) * 100, 1)

    # Color code
    def score_color(score):
        if score >= 80:
            return "#3fb950"
        elif score >= 50:
            return "#d29922"
        else:
            return "#f85149"

    dim_scores["color"] = dim_scores["score"].apply(score_color)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=dim_scores["dimension"],
        x=dim_scores["score"],
        orientation="h",
        marker_color=dim_scores["color"],
        text=dim_scores["score"].apply(lambda x: f"{x}%"),
        textposition="outside",
        textfont=dict(color="#f0f6fc", size=14),
    ))
    fig.update_layout(
        title=None,
        xaxis=dict(range=[0, 110], title="Score (%)", color="#8b949e", gridcolor="#21262d"),
        yaxis=dict(color="#f0f6fc"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=350,
        margin=dict(l=10, r=30, t=10, b=40),
        font=dict(color="#f0f6fc"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Detail table
    st.subheader("Szczegóły sprawdzeń")
    display_df = latest[["dimension", "check_name", "passed", "value", "threshold", "details"]].copy()
    display_df["status"] = display_df["passed"].apply(lambda x: "✅ Pass" if x else "❌ Fail")
    display_df = display_df[["status", "dimension", "check_name", "value", "threshold", "details"]]
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# ─── PAGE: Player Stats ───────────────────────────────────────

def page_player_stats():
    st.title("👥 Player Stats")

    try:
        stats_df = query_df("""
            SELECT player_name, team, minutes, goals, assists, xg, xg_assist, shots, source, season
            FROM player_match_stats
            ORDER BY xg DESC NULLS LAST
        """)
    except Exception:
        st.error("Brak danych. Uruchom: `python scripts/load_data.py`")
        return

    if stats_df.empty:
        st.warning("Brak statystyk w bazie.")
        return

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        sources = ["Wszystkie"] + sorted(stats_df["source"].dropna().unique().tolist())
        source_filter = st.selectbox("Źródło", sources)
    with col2:
        teams = ["Wszystkie"] + sorted(stats_df["team"].dropna().unique().tolist())
        team_filter = st.selectbox("Drużyna", teams)
    with col3:
        sort_by = st.selectbox("Sortuj wg", ["xg", "goals", "assists", "minutes", "shots"])

    filtered = stats_df.copy()
    if source_filter != "Wszystkie":
        filtered = filtered[filtered["source"] == source_filter]
    if team_filter != "Wszystkie":
        filtered = filtered[filtered["team"] == team_filter]

    filtered = filtered.sort_values(sort_by, ascending=False, na_position="last")

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Zawodnicy", len(filtered))
    with c2:
        st.metric("Suma goli", int(filtered["goals"].sum()) if not filtered["goals"].isna().all() else 0)
    with c3:
        st.metric("Avg xG", round(filtered["xg"].mean(), 2) if not filtered["xg"].isna().all() else "N/A")
    with c4:
        st.metric("Drużyn", filtered["team"].nunique())

    st.markdown("---")

    # Top 20 xG chart
    st.subheader(f"Top 20 – {sort_by.upper()}")
    top20 = filtered.head(20)

    fig = px.bar(
        top20, x=sort_by, y="player_name",
        orientation="h",
        color="source",
        color_discrete_map={"fbref": "#1f6feb", "understat": "#f85149"},
        hover_data=["team", "goals", "assists", "xg"],
    )
    fig.update_layout(
        yaxis=dict(autorange="reversed", color="#f0f6fc"),
        xaxis=dict(color="#8b949e", gridcolor="#21262d"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        height=500,
        margin=dict(l=10, r=10, t=10, b=40),
        font=dict(color="#f0f6fc"),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#f0f6fc")),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Full table
    st.subheader("Pełna tabela")
    st.dataframe(filtered, use_container_width=True, hide_index=True, height=400)


# ─── PAGE: Matches ─────────────────────────────────────────────

def page_matches():
    st.title("🏟️ Matches")

    try:
        matches_df = query_df("""
            SELECT date, home_team, away_team, home_score, away_score, league, season, source
            FROM matches
            ORDER BY date DESC
        """)
    except Exception:
        st.error("Brak danych meczów.")
        return

    if matches_df.empty:
        st.warning("Brak meczów w bazie.")
        return

    # Stats
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Total meczów", len(matches_df))
    with c2:
        st.metric("Źródła", matches_df["source"].nunique())
    with c3:
        st.metric("Najnowszy", matches_df["date"].max()[:10] if matches_df["date"].iloc[0] else "N/A")

    st.markdown("---")

    # Filter
    source_f = st.selectbox("Filtr źródła", ["Wszystkie"] + sorted(matches_df["source"].unique().tolist()))
    if source_f != "Wszystkie":
        matches_df = matches_df[matches_df["source"] == source_f]

    # Matches per source chart
    source_counts = matches_df.groupby("source").size().reset_index(name="count")
    fig = px.pie(source_counts, values="count", names="source",
                 color_discrete_sequence=["#1f6feb", "#f85149", "#3fb950"],
                 hole=0.4)
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#f0f6fc"),
        height=300,
    )

    col1, col2 = st.columns([1, 2])
    with col1:
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        st.dataframe(matches_df.head(50), use_container_width=True, hide_index=True, height=300)

    st.subheader("Wszystkie mecze")
    st.dataframe(matches_df, use_container_width=True, hide_index=True, height=500)


# ─── PAGE: DQ Details ──────────────────────────────────────────

def page_dq_details():
    st.title("🔍 DQ Details")

    try:
        dq_df = query_df("""
            SELECT check_name, dimension, passed, value, threshold, details, run_at
            FROM dq_check_results
            ORDER BY run_at DESC
        """)
    except Exception:
        st.error("Brak wyników DQ.")
        return

    if dq_df.empty:
        st.warning("Brak wyników DQ.")
        return

    # Filter by dimension
    dims = ["Wszystkie"] + sorted(dq_df["dimension"].unique().tolist())
    dim_filter = st.selectbox("Wymiar DQ", dims)

    filtered = dq_df.copy()
    if dim_filter != "Wszystkie":
        filtered = filtered[filtered["dimension"] == dim_filter]

    # Status breakdown
    st.subheader("Rozkład pass/fail per wymiar")

    dim_summary = dq_df.groupby(["dimension", "passed"]).size().reset_index(name="count")
    dim_summary["status"] = dim_summary["passed"].apply(lambda x: "Pass" if x else "Fail")

    fig = px.bar(
        dim_summary, x="dimension", y="count", color="status",
        barmode="stack",
        color_discrete_map={"Pass": "#3fb950", "Fail": "#f85149"},
    )
    fig.update_layout(
        xaxis=dict(color="#f0f6fc", gridcolor="#21262d"),
        yaxis=dict(title="Checks", color="#8b949e", gridcolor="#21262d"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#f0f6fc"),
        legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#f0f6fc")),
        height=350,
        margin=dict(l=10, r=10, t=10, b=40),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # DQ Dimension deep dive – values chart
    st.subheader("Wartości sprawdzeń")

    has_value = filtered[filtered["value"].notna()].copy()
    if not has_value.empty:
        has_value["color"] = has_value["passed"].apply(lambda x: "#3fb950" if x else "#f85149")

        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=has_value["check_name"],
            y=has_value["value"],
            marker_color=has_value["color"],
            text=has_value["value"].round(2),
            textposition="outside",
            textfont=dict(color="#f0f6fc"),
        ))
        # Add threshold line per check
        for _, row in has_value.iterrows():
            if row["threshold"] is not None:
                fig2.add_shape(
                    type="line",
                    x0=row["check_name"], x1=row["check_name"],
                    y0=0, y1=row["threshold"],
                    line=dict(color="#d29922", width=3, dash="dash"),
                )

        fig2.update_layout(
            xaxis=dict(color="#f0f6fc"),
            yaxis=dict(title="Value", color="#8b949e", gridcolor="#21262d"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#f0f6fc"),
            height=350,
            margin=dict(l=10, r=10, t=10, b=80),
        )
        st.plotly_chart(fig2, use_container_width=True)

    # Full results table
    st.subheader("Wszystkie wyniki")
    display = filtered.copy()
    display["status"] = display["passed"].apply(lambda x: "✅" if x else "❌")
    st.dataframe(
        display[["status", "dimension", "check_name", "value", "threshold", "details", "run_at"]],
        use_container_width=True, hide_index=True, height=400,
    )


# ─── Router ────────────────────────────────────────────────────

if page == "📊 DQ Scorecard":
    page_scorecard()
elif page == "👥 Player Stats":
    page_player_stats()
elif page == "🏟️ Matches":
    page_matches()
elif page == "🔍 DQ Details":
    page_dq_details()
