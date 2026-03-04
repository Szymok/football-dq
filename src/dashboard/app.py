"""
Football DQ Dashboard – Stadium Noir Edition

Design Direction: "Stadium Noir"
  Inspired by the atmosphere of a night match under floodlights.
  Deep pitch blacks, electric green pitch accents, warm amber floodlight glow.
  Memorable anchor: radial gauge DQ score meter.

DFII Score: 12/15
  Impact: 5, Fit: 4, Feasibility: 4, Performance: 4, Risk: 1

Uruchomienie:
    streamlit run src/dashboard/app.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import sqlite3
import math

from src.config.settings import DB_PATH

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

st.set_page_config(
    page_title="Football DQ",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DESIGN SYSTEM – Stadium Noir
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#
# Fonts: "Outfit" (display, geometric sans) + "JetBrains Mono" (data/mono)
# Dominant: Pitch Black (#08090d)
# Accent Primary: Electric Pitch Green (#00e676)
# Accent Secondary: Floodlight Amber (#ffab00)
# Danger: Match Red (#ff1744)
# Neutral: Fog Gray (#9ea7b8)
# Surface: Tunnel Dark (#111318)
# Border: Sideline (#1e2230)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;600&display=swap');

    :root {
        --bg-pitch: #08090d;
        --bg-tunnel: #111318;
        --bg-surface: #161a24;
        --border-sideline: #1e2230;
        --border-highlight: #2a3040;
        --accent-green: #00e676;
        --accent-amber: #ffab00;
        --danger-red: #ff1744;
        --text-primary: #edf0f7;
        --text-secondary: #9ea7b8;
        --text-muted: #5c6478;
        --font-display: 'Outfit', sans-serif;
        --font-mono: 'JetBrains Mono', monospace;
    }

    /* ── GLOBAL ── */
    .stApp {
        background: var(--bg-pitch) !important;
        font-family: var(--font-display);
    }

    .stApp > header { background: transparent !important; }

    /* ── SIDEBAR ── */
    section[data-testid="stSidebar"] {
        background: var(--bg-tunnel) !important;
        border-right: 1px solid var(--border-sideline) !important;
    }

    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown li,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] span {
        color: var(--text-secondary) !important;
        font-family: var(--font-display) !important;
    }

    section[data-testid="stSidebar"] .stRadio label span {
        font-weight: 500 !important;
        letter-spacing: 0.02em;
    }

    section[data-testid="stSidebar"] .stRadio [data-checked="true"] span {
        color: var(--accent-green) !important;
        font-weight: 600 !important;
    }

    /* ── TYPOGRAPHY ── */
    h1 {
        font-family: var(--font-display) !important;
        font-weight: 800 !important;
        color: var(--text-primary) !important;
        font-size: 2.2rem !important;
        letter-spacing: -0.03em !important;
        line-height: 1.1 !important;
        margin-bottom: 0.3em !important;
    }

    h2 {
        font-family: var(--font-display) !important;
        font-weight: 700 !important;
        color: var(--text-primary) !important;
        font-size: 1.3rem !important;
        letter-spacing: -0.01em !important;
    }

    h3 {
        font-family: var(--font-display) !important;
        font-weight: 600 !important;
        color: var(--text-secondary) !important;
        font-size: 1rem !important;
        text-transform: uppercase;
        letter-spacing: 0.12em !important;
    }

    p, span, li, div {
        font-family: var(--font-display) !important;
    }

    /* ── METRIC CARDS ── */
    div[data-testid="stMetric"] {
        background: var(--bg-surface) !important;
        border: 1px solid var(--border-sideline) !important;
        border-radius: 16px !important;
        padding: 20px 24px !important;
        box-shadow: 0 8px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.03) !important;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }

    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.5), inset 0 1px 0 rgba(255,255,255,0.05) !important;
    }

    div[data-testid="stMetric"] label {
        color: var(--text-muted) !important;
        font-size: 0.72rem !important;
        font-weight: 500 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
        font-family: var(--font-display) !important;
    }

    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--text-primary) !important;
        font-weight: 800 !important;
        font-size: 2rem !important;
        font-family: var(--font-display) !important;
        letter-spacing: -0.03em !important;
    }

    /* ── SELECTBOX / INPUTS ── */
    .stSelectbox > div > div {
        background: var(--bg-surface) !important;
        border: 1px solid var(--border-sideline) !important;
        border-radius: 10px !important;
        color: var(--text-primary) !important;
        font-family: var(--font-display) !important;
    }

    .stSelectbox label {
        color: var(--text-muted) !important;
        font-size: 0.72rem !important;
        text-transform: uppercase !important;
        letter-spacing: 0.08em !important;
        font-weight: 500 !important;
    }

    /* ── DATAFRAMES ── */
    .stDataFrame {
        border: 1px solid var(--border-sideline) !important;
        border-radius: 12px !important;
        overflow: hidden !important;
    }

    /* ── DIVIDERS ── */
    hr {
        border-color: var(--border-sideline) !important;
        margin: 1.5rem 0 !important;
    }

    /* ── HIDE STREAMLIT DEFAULTS ── */
    #MainMenu, footer, header[data-testid="stHeader"] { display: none !important; }

    /* ── CUSTOM BADGE ── */
    .dq-badge {
        display: inline-block;
        padding: 3px 10px;
        border-radius: 6px;
        font-size: 0.7rem;
        font-weight: 600;
        font-family: var(--font-mono);
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }
    .dq-badge.pass { background: rgba(0,230,118,0.12); color: #00e676; border: 1px solid rgba(0,230,118,0.2); }
    .dq-badge.fail { background: rgba(255,23,68,0.12); color: #ff1744; border: 1px solid rgba(255,23,68,0.2); }
    .dq-badge.warn { background: rgba(255,171,0,0.12); color: #ffab00; border: 1px solid rgba(255,171,0,0.2); }

    /* ── SCORE RING ── */
    .score-ring-container {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 8px;
    }
    .score-ring-label {
        font-family: var(--font-display);
        font-size: 0.7rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: var(--text-muted);
    }

    /* ── DIM CARD ── */
    .dim-card {
        background: var(--bg-surface);
        border: 1px solid var(--border-sideline);
        border-radius: 14px;
        padding: 20px;
        margin-bottom: 12px;
        box-shadow: 0 4px 16px rgba(0,0,0,0.3);
    }
    .dim-card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 12px;
    }
    .dim-card-title {
        font-family: var(--font-display);
        font-weight: 600;
        font-size: 0.95rem;
        color: var(--text-primary);
        text-transform: capitalize;
    }
    .dim-card-bar-bg {
        height: 8px;
        background: var(--border-sideline);
        border-radius: 4px;
        overflow: hidden;
    }
    .dim-card-bar-fill {
        height: 100%;
        border-radius: 4px;
        transition: width 0.6s ease;
    }
    .dim-card-score {
        font-family: var(--font-mono);
        font-size: 0.85rem;
        font-weight: 600;
    }

    /* ── CHECK ROW ── */
    .check-row {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 12px 16px;
        background: var(--bg-surface);
        border: 1px solid var(--border-sideline);
        border-radius: 10px;
        margin-bottom: 8px;
    }
    .check-row:hover { border-color: var(--border-highlight); }
    .check-name {
        font-family: var(--font-mono);
        font-size: 0.82rem;
        color: var(--text-primary);
        flex: 1;
    }
    .check-dim {
        font-family: var(--font-display);
        font-size: 0.7rem;
        color: var(--text-muted);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        width: 110px;
    }
    .check-detail {
        font-family: var(--font-display);
        font-size: 0.75rem;
        color: var(--text-secondary);
        flex: 2;
    }
</style>
""", unsafe_allow_html=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PLOTLY THEME
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PLOTLY_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Outfit, sans-serif", color="#9ea7b8", size=12),
    margin=dict(l=0, r=0, t=24, b=0),
    xaxis=dict(gridcolor="#1e2230", zerolinecolor="#1e2230"),
    yaxis=dict(gridcolor="#1e2230", zerolinecolor="#1e2230"),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color="#9ea7b8")),
)

COLORS = {
    "green": "#00e676",
    "amber": "#ffab00",
    "red": "#ff1744",
    "blue": "#448aff",
    "purple": "#b388ff",
    "fbref": "#448aff",
    "understat": "#ff6e40",
}


def apply_theme(fig, **overrides):
    layout = {**PLOTLY_LAYOUT, **overrides}
    fig.update_layout(**layout)
    return fig


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@st.cache_resource
def get_connection():
    return sqlite3.connect(str(DB_PATH), check_same_thread=False)


@st.cache_data(ttl=60)
def query_df(sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, get_connection())


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# COMPONENTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def score_color(score):
    if score >= 80: return COLORS["green"]
    if score >= 50: return COLORS["amber"]
    return COLORS["red"]


def render_gauge(score, size=220):
    """Radial gauge – the memorable design anchor."""
    color = score_color(score)
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        number=dict(
            suffix="%",
            font=dict(family="Outfit", size=44, color="#edf0f7", weight=800),
        ),
        gauge=dict(
            axis=dict(range=[0, 100], visible=False),
            bar=dict(color=color, thickness=0.82),
            bgcolor="#1e2230",
            borderwidth=0,
            shape="angular",
            steps=[
                dict(range=[0, 50], color="rgba(255,23,68,0.06)"),
                dict(range=[50, 80], color="rgba(255,171,0,0.06)"),
                dict(range=[80, 100], color="rgba(0,230,118,0.06)"),
            ],
            threshold=dict(line=dict(color=color, width=3), thickness=0.9, value=score),
        ),
    ))
    fig.update_layout(
        height=size, width=size,
        margin=dict(l=20, r=20, t=30, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Outfit"),
    )
    return fig


def render_dim_card(name, score, checks_passed, checks_total):
    """Custom HTML dimension card with progress bar."""
    color = score_color(score)
    badge_class = "pass" if score >= 80 else ("warn" if score >= 50 else "fail")
    return f"""
    <div class="dim-card">
        <div class="dim-card-header">
            <span class="dim-card-title">{name}</span>
            <span class="dq-badge {badge_class}">{checks_passed}/{checks_total}</span>
        </div>
        <div class="dim-card-bar-bg">
            <div class="dim-card-bar-fill" style="width:{score}%; background:{color};"></div>
        </div>
        <div style="display:flex; justify-content:space-between; margin-top:8px;">
            <span class="dim-card-score" style="color:{color};">{score}%</span>
            <span style="font-family:var(--font-display); font-size:0.72rem; color:var(--text-muted);">quality score</span>
        </div>
    </div>
    """


def render_check_row(status, dimension, name, details):
    """Custom HTML check result row."""
    badge = "pass" if status else "fail"
    icon = "●" if status else "○"
    icon_color = COLORS["green"] if status else COLORS["red"]
    detail_text = (details[:90] + "...") if details and len(details) > 90 else (details or "")
    return f"""
    <div class="check-row">
        <span style="color:{icon_color}; font-size:1.1rem;">{icon}</span>
        <span class="check-dim">{dimension}</span>
        <span class="check-name">{name}</span>
        <span class="check-detail">{detail_text}</span>
        <span class="dq-badge {badge}">{"pass" if status else "fail"}</span>
    </div>
    """


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SIDEBAR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

with st.sidebar:
    st.markdown("""
    <div style="padding: 8px 0 16px 0;">
        <span style="font-family: var(--font-display); font-weight: 800; font-size: 1.4rem;
              color: var(--accent-green); letter-spacing: -0.03em;">football</span><span
              style="font-family: var(--font-display); font-weight: 300; font-size: 1.4rem;
              color: var(--text-muted); letter-spacing: -0.03em;">dq</span>
        <br>
        <span style="font-family: var(--font-mono); font-size: 0.65rem; color: var(--text-muted);
              letter-spacing: 0.08em; text-transform: uppercase;">data quality monitor</span>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["Scorecard", "Players", "Matches", "Checks"],
        index=0,
        label_visibility="collapsed",
    )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("""
    <div style="padding:12px 0; border-top: 1px solid var(--border-sideline);">
        <span style="font-family:var(--font-mono); font-size:0.65rem; color:var(--text-muted);
              text-transform:uppercase; letter-spacing:0.1em;">sources</span>
        <div style="margin-top:8px; display:flex; gap:6px; flex-wrap:wrap;">
            <span style="font-family:var(--font-mono); font-size:0.7rem; padding:3px 8px;
                  background:rgba(68,138,255,0.1); color:#448aff; border-radius:4px;
                  border:1px solid rgba(68,138,255,0.2);">FBref</span>
            <span style="font-family:var(--font-mono); font-size:0.7rem; padding:3px 8px;
                  background:rgba(255,110,64,0.1); color:#ff6e40; border-radius:4px;
                  border:1px solid rgba(255,110,64,0.2);">Understat</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    try:
        counts = query_df("""
            SELECT
                (SELECT count(*) FROM player_match_stats) as stats,
                (SELECT count(*) FROM matches) as matches,
                (SELECT count(*) FROM dq_check_results) as checks
        """).iloc[0]
        st.markdown(f"""
        <div style="padding:12px 0; border-top: 1px solid var(--border-sideline);">
            <span style="font-family:var(--font-mono); font-size:0.65rem; color:var(--text-muted);
                  text-transform:uppercase; letter-spacing:0.1em;">database</span>
            <div style="margin-top:6px; font-family:var(--font-mono); font-size:0.75rem; color:var(--text-secondary);">
                {int(counts['stats'])} stats &middot; {int(counts['matches'])} matches &middot; {int(counts['checks'])} checks
            </div>
        </div>
        """, unsafe_allow_html=True)
    except Exception:
        pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PAGE: SCORECARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def page_scorecard():
    st.markdown('<h1>Data Quality<br><span style="color: var(--accent-green);">Scorecard</span></h1>',
                unsafe_allow_html=True)

    try:
        dq_df = query_df("""
            SELECT check_name, dimension, passed, value, threshold, details, run_at
            FROM dq_check_results ORDER BY run_at DESC
        """)
    except Exception:
        st.error("No DQ results. Run: `python scripts/run_dq.py`")
        return

    if dq_df.empty:
        return

    latest_run = dq_df["run_at"].max()
    latest = dq_df[dq_df["run_at"] == latest_run]

    total = len(latest)
    passed = int(latest["passed"].sum())
    failed = total - passed
    score = round((passed / total) * 100, 1) if total else 0

    # ── Hero: Gauge + KPIs ──
    gauge_col, kpi_col = st.columns([1, 2], gap="large")

    with gauge_col:
        st.markdown('<div class="score-ring-container">', unsafe_allow_html=True)
        st.plotly_chart(render_gauge(score, 240), use_container_width=False,
                        config={"displayModeBar": False})
        st.markdown(f'<span class="score-ring-label">overall quality</span>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with kpi_col:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Checks", total)
        c2.metric("Passed", passed)
        c3.metric("Failed", failed)

        st.markdown(f"""
        <div style="margin-top:16px; padding:14px 18px; background:var(--bg-surface);
             border:1px solid var(--border-sideline); border-radius:12px;">
            <span style="font-family:var(--font-mono); font-size:0.7rem; color:var(--text-muted);
                  text-transform:uppercase; letter-spacing:0.08em;">last run</span>
            <span style="font-family:var(--font-mono); font-size:0.82rem; color:var(--text-secondary);
                  margin-left:12px;">{latest_run[:19]}</span>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Dimension Cards ──
    st.markdown('<h3>Quality Dimensions</h3>', unsafe_allow_html=True)

    dim_data = latest.groupby("dimension").agg(
        total=("passed", "count"),
        passed=("passed", "sum"),
    ).reset_index()
    dim_data["score"] = round((dim_data["passed"] / dim_data["total"]) * 100, 1)
    dim_data = dim_data.sort_values("score", ascending=False)

    cols = st.columns(3)
    for i, (_, row) in enumerate(dim_data.iterrows()):
        with cols[i % 3]:
            st.markdown(render_dim_card(
                row["dimension"], row["score"], int(row["passed"]), int(row["total"])
            ), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Check Results ──
    st.markdown('<h3>All Checks</h3>', unsafe_allow_html=True)

    for _, row in latest.iterrows():
        st.markdown(render_check_row(
            bool(row["passed"]), row["dimension"], row["check_name"], row["details"]
        ), unsafe_allow_html=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PAGE: PLAYERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def page_players():
    st.markdown('<h1>Player<br><span style="color: var(--accent-amber);">Statistics</span></h1>',
                unsafe_allow_html=True)

    try:
        df = query_df("""
            SELECT player_name, team, minutes, goals, assists, xg, xg_assist, shots, source, season
            FROM player_match_stats ORDER BY xg DESC NULLS LAST
        """)
    except Exception:
        st.error("No data. Run: `python scripts/load_data.py`")
        return

    if df.empty:
        return

    # ── Filters ──
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        sources = ["All"] + sorted(df["source"].dropna().unique().tolist())
        src = st.selectbox("Source", sources, label_visibility="visible")
    with fc2:
        teams = ["All"] + sorted(df["team"].dropna().unique().tolist())
        team = st.selectbox("Team", teams)
    with fc3:
        metric = st.selectbox("Sort by", ["xg", "goals", "assists", "minutes", "shots"])

    filtered = df.copy()
    if src != "All":
        filtered = filtered[filtered["source"] == src]
    if team != "All":
        filtered = filtered[filtered["team"] == team]
    filtered = filtered.sort_values(metric, ascending=False, na_position="last")

    # ── KPIs ──
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Players", len(filtered))
    c2.metric("Goals", int(filtered["goals"].sum()) if not filtered["goals"].isna().all() else 0)
    c3.metric("Avg xG", round(filtered["xg"].mean(), 2) if not filtered["xg"].isna().all() else "—")
    c4.metric("Teams", filtered["team"].nunique())

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Top 15 Chart ──
    top = filtered.head(15)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=top["player_name"],
        x=top[metric],
        orientation="h",
        marker=dict(
            color=[COLORS.get(s, "#448aff") for s in top["source"]],
            line=dict(width=0),
        ),
        text=top[metric].round(1),
        textposition="outside",
        textfont=dict(family="JetBrains Mono", size=11, color="#edf0f7"),
        hovertemplate="<b>%{y}</b><br>" + metric + ": %{x:.1f}<extra></extra>",
    ))
    apply_theme(fig, height=440, margin=dict(l=0, r=40, t=8, b=0))
    fig.update_yaxes(autorange="reversed", tickfont=dict(size=11, color="#edf0f7"))
    fig.update_xaxes(title=metric.upper(), title_font=dict(size=11, color="#5c6478"))
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # ── Table ──
    st.markdown('<h3>Full Dataset</h3>', unsafe_allow_html=True)
    st.dataframe(
        filtered[["player_name", "team", "goals", "assists", "xg", "xg_assist", "minutes", "shots", "source"]],
        use_container_width=True, hide_index=True, height=420,
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PAGE: MATCHES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def page_matches():
    st.markdown('<h1>Match<br><span style="color: var(--accent-green);">Schedule</span></h1>',
                unsafe_allow_html=True)

    try:
        df = query_df("""
            SELECT date, home_team, away_team, home_score, away_score, league, season, source
            FROM matches ORDER BY date DESC
        """)
    except Exception:
        st.error("No match data.")
        return

    if df.empty:
        return

    # ── KPIs ──
    c1, c2, c3 = st.columns(3)
    c1.metric("Matches", len(df))
    c2.metric("Sources", df["source"].nunique())
    latest_date = df["date"].max()
    c3.metric("Latest", latest_date[:10] if latest_date else "—")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Source filter ──
    src = st.selectbox("Filter by source", ["All"] + sorted(df["source"].unique().tolist()))
    if src != "All":
        df = df[df["source"] == src]

    # ── Source distribution ──
    col_chart, col_table = st.columns([1, 2], gap="large")

    with col_chart:
        src_counts = df.groupby("source").size().reset_index(name="count")
        fig = go.Figure(go.Pie(
            labels=src_counts["source"],
            values=src_counts["count"],
            hole=0.55,
            marker=dict(colors=[COLORS.get(s, "#448aff") for s in src_counts["source"]]),
            textfont=dict(family="Outfit", size=12, color="#edf0f7"),
            hovertemplate="<b>%{label}</b><br>%{value} matches<extra></extra>",
        ))
        apply_theme(fig, height=280, margin=dict(l=10, r=10, t=10, b=10))
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    with col_table:
        st.dataframe(df.head(50), use_container_width=True, hide_index=True, height=280)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<h3>All Matches</h3>', unsafe_allow_html=True)
    st.dataframe(df, use_container_width=True, hide_index=True, height=500)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PAGE: CHECKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def page_checks():
    st.markdown('<h1>Quality<br><span style="color: var(--danger-red);">Deep Dive</span></h1>',
                unsafe_allow_html=True)

    try:
        dq_df = query_df("""
            SELECT check_name, dimension, passed, value, threshold, details, run_at
            FROM dq_check_results ORDER BY run_at DESC
        """)
    except Exception:
        st.error("No DQ data.")
        return

    if dq_df.empty:
        return

    # ── Filter ──
    dims = ["All"] + sorted(dq_df["dimension"].unique().tolist())
    dim_f = st.selectbox("Dimension", dims)

    filtered = dq_df if dim_f == "All" else dq_df[dq_df["dimension"] == dim_f]

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Stacked dimension chart ──
    st.markdown('<h3>Pass / Fail Breakdown</h3>', unsafe_allow_html=True)

    summary = dq_df.groupby(["dimension", "passed"]).size().reset_index(name="count")
    summary["status"] = summary["passed"].apply(lambda x: "Pass" if x else "Fail")

    fig = go.Figure()
    for status, color in [("Pass", COLORS["green"]), ("Fail", COLORS["red"])]:
        subset = summary[summary["status"] == status]
        fig.add_trace(go.Bar(
            x=subset["dimension"], y=subset["count"], name=status,
            marker=dict(color=color, line=dict(width=0)),
            texttemplate="%{y}", textposition="inside",
            textfont=dict(family="JetBrains Mono", size=11),
        ))
    apply_theme(fig, barmode="stack", height=320, margin=dict(l=0, r=0, t=8, b=0))
    fig.update_xaxes(tickfont=dict(size=11, color="#edf0f7"))
    fig.update_yaxes(title="Checks", title_font=dict(size=11, color="#5c6478"))
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Values chart ──
    has_val = filtered[filtered["value"].notna()].copy()
    if not has_val.empty:
        st.markdown('<h3>Check Values</h3>', unsafe_allow_html=True)

        fig2 = go.Figure()
        colors = [COLORS["green"] if p else COLORS["red"] for p in has_val["passed"]]
        fig2.add_trace(go.Bar(
            x=has_val["check_name"], y=has_val["value"],
            marker=dict(color=colors, line=dict(width=0)),
            text=has_val["value"].round(2),
            textposition="outside",
            textfont=dict(family="JetBrains Mono", size=11, color="#edf0f7"),
        ))

        # Threshold markers
        for _, row in has_val.iterrows():
            if row["threshold"] is not None and row["threshold"] > 0:
                fig2.add_shape(
                    type="line",
                    x0=-0.5, x1=len(has_val) - 0.5,
                    y0=row["threshold"], y1=row["threshold"],
                    line=dict(color=COLORS["amber"], width=1.5, dash="dot"),
                )

        apply_theme(fig2, height=320, margin=dict(l=0, r=0, t=8, b=60))
        fig2.update_xaxes(tickangle=-35, tickfont=dict(size=10, color="#9ea7b8"))
        fig2.update_yaxes(title="Value", title_font=dict(size=11, color="#5c6478"))
        st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Results list ──
    st.markdown('<h3>All Results</h3>', unsafe_allow_html=True)
    for _, row in filtered.iterrows():
        st.markdown(render_check_row(
            bool(row["passed"]), row["dimension"], row["check_name"], row["details"]
        ), unsafe_allow_html=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ROUTER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{"Scorecard": page_scorecard, "Players": page_players,
 "Matches": page_matches, "Checks": page_checks}[page]()
