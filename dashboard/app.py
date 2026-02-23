"""
Solaris Media — Fraud Detection Dashboard
Run: streamlit run dashboard/app.py
"""

import json
import os
import io

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

# ── Config ────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Solaris Fraud Detection",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

RISK_COLORS = {
    "LOW": "#4CAF50",
    "MEDIUM": "#FF9800",
    "HIGH": "#F44336",
    "CRITICAL": "#9C27B0",
}

DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "enriched_transactions.csv",
)

# ── Data loader ───────────────────────────────────────────────────────────────

@st.cache_resource
def get_duckdb_conn():
    """Persistent DuckDB in-memory connection, shared across Streamlit reruns."""
    conn = duckdb.connect()
    conn.execute(f"""
        CREATE OR REPLACE VIEW transactions AS
        SELECT * FROM read_csv_auto('{DATA_PATH}')
    """)
    return conn


@st.cache_data
def load_data() -> pd.DataFrame:
    """Load enriched transactions via DuckDB and add derived columns."""
    conn = get_duckdb_conn()
    df = conn.execute("""
        SELECT *
        FROM transactions
        ORDER BY timestamp
    """).df()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    df["dow"] = df["timestamp"].dt.day_name()
    df["signals_list"] = df["signals_triggered"].apply(
        lambda x: json.loads(x) if pd.notna(x) else []
    )
    df["signal_count"] = df["signals_list"].apply(len)
    return df


def query(sql: str, df: pd.DataFrame = None) -> pd.DataFrame:
    """Run an analytical SQL query via DuckDB. Optionally register a DataFrame as 'df'."""
    conn = duckdb.connect()
    if df is not None:
        conn.register("df", df)
    return conn.execute(sql).df()


def check_data_exists() -> bool:
    return os.path.exists(DATA_PATH)


# ── Sidebar ───────────────────────────────────────────────────────────────────

def render_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.image(
        "https://img.shields.io/badge/Solaris%20Media-Fraud%20Detection-purple",
        use_container_width=True,
    )
    st.sidebar.title("🔍 Filters")

    min_date = df["timestamp"].min().date()
    max_date = df["timestamp"].max().date()
    date_range = st.sidebar.date_input(
        "Date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    countries = sorted(df["country"].unique())
    selected_countries = st.sidebar.multiselect("Country", countries, default=countries)

    tiers = sorted(df["subscription_tier"].unique())
    selected_tiers = st.sidebar.multiselect("Subscription Tier", tiers, default=tiers)

    risk_levels = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    selected_risks = st.sidebar.multiselect("Risk Level", risk_levels, default=risk_levels)

    statuses = sorted(df["status"].unique())
    selected_statuses = st.sidebar.multiselect("Status", statuses, default=statuses)

    # Apply filters
    filtered = df.copy()
    if len(date_range) == 2:
        start_d, end_d = date_range
        filtered = filtered[
            (filtered["timestamp"].dt.date >= start_d)
            & (filtered["timestamp"].dt.date <= end_d)
        ]
    if selected_countries:
        filtered = filtered[filtered["country"].isin(selected_countries)]
    if selected_tiers:
        filtered = filtered[filtered["subscription_tier"].isin(selected_tiers)]
    if selected_risks:
        filtered = filtered[filtered["risk_level"].isin(selected_risks)]
    if selected_statuses:
        filtered = filtered[filtered["status"].isin(selected_statuses)]

    # Export button
    st.sidebar.markdown("---")
    csv_buffer = io.StringIO()
    filtered.drop(columns=["signals_list"], errors="ignore").to_csv(csv_buffer, index=False)
    st.sidebar.download_button(
        "⬇️ Export Filtered CSV",
        data=csv_buffer.getvalue().encode("utf-8"),
        file_name="filtered_transactions.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.sidebar.markdown(f"**{len(filtered):,}** transactions shown")
    return filtered


# ── KPI Cards ─────────────────────────────────────────────────────────────────

def render_kpis(df: pd.DataFrame) -> None:
    total = len(df)
    fraud_txns = df[df["status"].isin(["declined_fraud", "chargeback"])]
    fraud_rate = len(fraud_txns) / total * 100 if total else 0
    chargebacks = len(df[df["status"] == "chargeback"])
    avg_risk = df["risk_score"].mean() if total else 0
    critical_count = len(df[df["risk_level"] == "CRITICAL"])

    cols = st.columns(5)
    cols[0].metric("Total Transactions", f"{total:,}")
    cols[1].metric("Fraud Rate", f"{fraud_rate:.1f}%", delta_color="inverse")
    cols[2].metric("Chargebacks", f"{chargebacks:,}", delta_color="inverse")
    cols[3].metric("Avg Risk Score", f"{avg_risk:.1f}")
    cols[4].metric(
        "🚨 CRITICAL",
        f"{critical_count:,}",
        delta=f"{critical_count/total*100:.1f}% of total" if total else "0%",
        delta_color="inverse",
    )


# ── Time Series ───────────────────────────────────────────────────────────────

def render_time_series(df: pd.DataFrame) -> None:
    st.subheader("📈 Transaction Volume & Fraud Score Over Time")

    # DuckDB: volume per day × risk level + avg score per day
    daily_risk = query("""
        SELECT
            CAST(timestamp AS DATE)          AS date,
            risk_level,
            COUNT(*)                         AS volume
        FROM df
        GROUP BY CAST(timestamp AS DATE), risk_level
        ORDER BY date, risk_level
    """, df)

    daily_score = query("""
        SELECT
            CAST(timestamp AS DATE)          AS date,
            ROUND(AVG(risk_score), 2)        AS avg_risk
        FROM df
        GROUP BY CAST(timestamp AS DATE)
        ORDER BY date
    """, df)

    # Stacked bars: one bar per day, split by risk level, colored by risk
    risk_order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    for level in risk_order:
        level_data = daily_risk[daily_risk["risk_level"] == level]
        fig.add_trace(
            go.Bar(
                x=level_data["date"],
                y=level_data["volume"],
                name=level,
                marker_color=RISK_COLORS[level],
                opacity=0.85,
                legendgroup=level,
            ),
            secondary_y=False,
        )

    # Avg risk score line on secondary axis
    fig.add_trace(
        go.Scatter(
            x=daily_score["date"],
            y=daily_score["avg_risk"],
            name="Avg Risk Score",
            line=dict(color="#FFFFFF", width=2.5, dash="dot"),
            mode="lines+markers",
            marker=dict(size=5),
        ),
        secondary_y=True,
    )

    fig.update_layout(
        barmode="stack",
        height=380,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.update_yaxes(title_text="Transaction Volume (by Risk Level)", secondary_y=False)
    fig.update_yaxes(title_text="Avg Risk Score", secondary_y=True, showgrid=False)

    st.plotly_chart(fig, use_container_width=True)


# ── Risk Distribution ─────────────────────────────────────────────────────────

def render_risk_distribution(df: pd.DataFrame) -> None:
    st.subheader("🎯 Risk Distribution")
    col1, col2 = st.columns(2)

    # Donut chart
    level_counts = df["risk_level"].value_counts().reindex(
        ["LOW", "MEDIUM", "HIGH", "CRITICAL"], fill_value=0
    )
    fig_donut = go.Figure(
        go.Pie(
            labels=level_counts.index,
            values=level_counts.values,
            hole=0.5,
            marker_colors=[RISK_COLORS[l] for l in level_counts.index],
        )
    )
    fig_donut.update_layout(
        title="Risk Level Breakdown",
        height=320,
        showlegend=True,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    col1.plotly_chart(fig_donut, use_container_width=True)

    # Stacked bar by country
    country_risk = (
        df.groupby(["country", "risk_level"])
        .size()
        .reset_index(name="count")
    )
    fig_bar = px.bar(
        country_risk,
        x="country",
        y="count",
        color="risk_level",
        color_discrete_map=RISK_COLORS,
        title="Transactions by Country & Risk Level",
        category_orders={"risk_level": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]},
        height=320,
    )
    fig_bar.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    col2.plotly_chart(fig_bar, use_container_width=True)


# ── Top Anomalies Table ───────────────────────────────────────────────────────

def render_anomalies_table(df: pd.DataFrame) -> None:
    st.subheader("🚨 Top 20 Anomalies by Risk Score")

    top20 = (
        df.nlargest(20, "risk_score")[
            [
                "timestamp", "customer_email", "amount", "currency",
                "country", "risk_level", "risk_score", "signals_triggered", "status",
            ]
        ]
        .copy()
    )
    top20["timestamp"] = top20["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
    top20["signals_triggered"] = top20["signals_triggered"].apply(
        lambda x: ", ".join(json.loads(x)) if pd.notna(x) else ""
    )

    def highlight_critical(row):
        if row["risk_level"] == "CRITICAL":
            return ["background-color: #4a0030; color: white"] * len(row)
        elif row["risk_level"] == "HIGH":
            return ["background-color: #4a1000; color: white"] * len(row)
        return [""] * len(row)

    styled = top20.style.apply(highlight_critical, axis=1).format(
        {"amount": "{:.2f}", "risk_score": "{:.0f}"}
    )
    st.dataframe(styled, use_container_width=True, height=500)


# ── Pattern Insights Tabs ─────────────────────────────────────────────────────

def render_pattern_insights(df: pd.DataFrame) -> None:
    st.subheader("🔬 Pattern Insights")
    tab_bins, tab_ips, tab_horario, tab_geo = st.tabs(
        ["💳 BINs", "🌐 IPs", "🕐 Horario", "🗺️ Geo"]
    )

    with tab_bins:
        _render_bins_tab(df)

    with tab_ips:
        _render_ips_tab(df)

    with tab_horario:
        _render_hourly_tab(df)

    with tab_geo:
        _render_geo_tab(df)


def _render_bins_tab(df: pd.DataFrame) -> None:
    bin_stats = query("""
        SELECT
            card_bin,
            COUNT(*)                                                                         AS total,
            ROUND(AVG(risk_score), 1)                                                        AS avg_risk,
            SUM(CASE WHEN status IN ('declined_fraud','declined_insufficient_funds')
                     THEN 1 ELSE 0 END)                                                      AS declines,
            SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END)                          AS chargebacks,
            ROUND(CAST(SUM(CASE WHEN status IN ('declined_fraud','declined_insufficient_funds')
                              THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100, 1)            AS decline_rate
        FROM df
        GROUP BY card_bin
        ORDER BY avg_risk DESC
    """, df)
    top_bins = bin_stats.head(10)

    fig = px.bar(
        top_bins,
        x="card_bin",
        y="avg_risk",
        color="decline_rate",
        color_continuous_scale="Reds",
        title="Top 10 BINs by Average Risk Score",
        labels={"avg_risk": "Avg Risk Score", "card_bin": "BIN", "decline_rate": "Decline %"},
        height=350,
    )
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**BIN Detail Table**")
    display_cols = bin_stats.nlargest(15, "avg_risk")[
        ["card_bin", "total", "declines", "chargebacks", "decline_rate", "avg_risk"]
    ].copy()
    display_cols.columns = ["BIN", "Total Txns", "Declines", "Chargebacks", "Decline Rate %", "Avg Risk"]
    st.dataframe(display_cols.reset_index(drop=True), use_container_width=True)


def _render_ips_tab(df: pd.DataFrame) -> None:
    ip_stats = query("""
        SELECT
            ip_address,
            COUNT(*)                                                                          AS total_txns,
            COUNT(DISTINCT card_last4)                                                        AS unique_cards,
            COUNT(DISTINCT card_bin)                                                          AS unique_bins,
            ROUND(AVG(risk_score), 1)                                                         AS avg_risk,
            SUM(CASE WHEN status IN ('declined_fraud','declined_insufficient_funds')
                     THEN 1 ELSE 0 END)                                                       AS declines
        FROM df
        GROUP BY ip_address
        ORDER BY unique_cards DESC
        LIMIT 15
    """, df)
    top_ips = ip_stats

    fig = px.bar(
        top_ips.head(10),
        x="ip_address",
        y="unique_cards",
        color="avg_risk",
        color_continuous_scale="Reds",
        title="Top IPs by Unique Cards (Card Testing Indicator)",
        labels={"unique_cards": "Unique Cards", "ip_address": "IP Address", "avg_risk": "Avg Risk"},
        height=350,
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("**Suspicious IPs (>3 unique cards)**")
    suspicious = top_ips[top_ips["unique_cards"] > 3][
        ["ip_address", "total_txns", "unique_cards", "unique_bins", "declines", "avg_risk"]
    ].copy()
    suspicious.columns = ["IP", "Total Txns", "Unique Cards", "Unique BINs", "Declines", "Avg Risk"]
    st.dataframe(suspicious.reset_index(drop=True), use_container_width=True)


def _render_hourly_tab(df: pd.DataFrame) -> None:
    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    heatmap_data = (
        df.groupby(["dow", "hour"])
        .agg(avg_risk=("risk_score", "mean"), count=("transaction_id", "count"))
        .reset_index()
    )

    pivot = heatmap_data.pivot(index="dow", columns="hour", values="avg_risk").reindex(
        [d for d in dow_order if d in heatmap_data["dow"].unique()]
    )

    fig = px.imshow(
        pivot,
        color_continuous_scale="RdYlGn_r",
        aspect="auto",
        title="Fraud Risk Heatmap: Hour of Day × Day of Week",
        labels={"x": "Hour of Day", "y": "Day of Week", "color": "Avg Risk Score"},
        height=380,
    )
    fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig, use_container_width=True)

    # Volume heatmap
    pivot_vol = heatmap_data.pivot(index="dow", columns="hour", values="count").reindex(
        [d for d in dow_order if d in heatmap_data["dow"].unique()]
    )
    fig2 = px.imshow(
        pivot_vol,
        color_continuous_scale="Blues",
        aspect="auto",
        title="Transaction Volume Heatmap",
        labels={"x": "Hour of Day", "y": "Day of Week", "color": "Count"},
        height=380,
    )
    fig2.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig2, use_container_width=True)


def _render_geo_tab(df: pd.DataFrame) -> None:
    FOREIGN_PREFIXES = ("5.188.", "185.220.", "193.32.", "45.142.", "91.108.")
    geo_mismatch = df[
        df["ip_address"].str.startswith(FOREIGN_PREFIXES) | (df["bin_country"] != df["country"])
    ].copy()

    st.metric("Geo Mismatch Transactions", len(geo_mismatch))

    if not geo_mismatch.empty:
        by_country = (
            geo_mismatch.groupby(["country", "bin_country"])
            .agg(count=("transaction_id", "count"), avg_risk=("risk_score", "mean"))
            .reset_index()
            .sort_values("count", ascending=False)
        )
        by_country.columns = ["Transaction Country", "BIN Country", "Count", "Avg Risk Score"]
        st.dataframe(by_country, use_container_width=True)

        fig = px.bar(
            geo_mismatch.groupby("country").size().reset_index(name="count"),
            x="country",
            y="count",
            color="count",
            color_continuous_scale="Reds",
            title="Geo Mismatch Count by Transaction Country",
            height=320,
        )
        fig.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Sample Geo Mismatch Transactions**")
        sample = geo_mismatch.nlargest(20, "risk_score")[
            ["timestamp", "customer_email", "country", "bin_country", "ip_address", "risk_score", "status"]
        ].copy()
        sample["timestamp"] = sample["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(sample.reset_index(drop=True), use_container_width=True)
    else:
        st.info("No geo mismatches found in current filter.")


# ── Export Report ─────────────────────────────────────────────────────────────

def render_export_section(df: pd.DataFrame) -> None:
    st.subheader("📤 Export Fraud Report")

    col1, col2 = st.columns(2)

    # Build summary report
    def build_report(df: pd.DataFrame) -> dict:
        top_bins = (
            df.groupby("card_bin")
            .agg(
                total=("transaction_id", "count"),
                avg_risk=("risk_score", "mean"),
                decline_rate=(
                    "status",
                    lambda s: round(s.isin(["declined_fraud", "declined_insufficient_funds"]).mean() * 100, 1),
                ),
            )
            .nlargest(10, "avg_risk")
            .reset_index()
            .to_dict("records")
        )

        top_ips = (
            df.groupby("ip_address")
            .agg(
                unique_cards=("card_last4", "nunique"),
                total_txns=("transaction_id", "count"),
                avg_risk=("risk_score", "mean"),
            )
            .nlargest(10, "unique_cards")
            .reset_index()
            .to_dict("records")
        )

        hourly = (
            df.groupby("hour")
            .agg(count=("transaction_id", "count"), avg_risk=("risk_score", "mean"))
            .reset_index()
            .to_dict("records")
        )

        FOREIGN_PREFIXES = ("5.188.", "185.220.", "193.32.", "45.142.", "91.108.")
        geo_mismatch = df[
            df["ip_address"].str.startswith(FOREIGN_PREFIXES) | (df["bin_country"] != df["country"])
        ]
        geo_summary = (
            geo_mismatch.groupby(["country", "bin_country"])
            .size()
            .reset_index(name="count")
            .to_dict("records")
        )

        return {
            "generated_at": pd.Timestamp.now().isoformat(),
            "total_transactions": len(df),
            "critical_count": int((df["risk_level"] == "CRITICAL").sum()),
            "fraud_rate_pct": round(
                df["status"].isin(["declined_fraud", "chargeback"]).mean() * 100, 2
            ),
            "top_bins": top_bins,
            "top_ips_card_testing": top_ips,
            "hourly_patterns": hourly,
            "geo_anomalies": geo_summary,
        }

    report_data = build_report(df)

    # JSON download
    json_str = json.dumps(report_data, indent=2, default=str)
    col1.download_button(
        "⬇️ Download fraud_pattern_report.json",
        data=json_str.encode("utf-8"),
        file_name="fraud_pattern_report.json",
        mime="application/json",
        use_container_width=True,
    )

    # CSV download (top anomalies)
    report_df = df.nlargest(100, "risk_score")[
        ["transaction_id", "timestamp", "customer_email", "amount", "currency",
         "country", "card_bin", "ip_address", "risk_score", "risk_level", "signals_triggered", "status"]
    ].copy()
    report_df["timestamp"] = report_df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    csv_buf = io.StringIO()
    report_df.to_csv(csv_buf, index=False)
    col2.download_button(
        "⬇️ Download fraud_pattern_report.csv",
        data=csv_buf.getvalue().encode("utf-8"),
        file_name="fraud_pattern_report.csv",
        mime="text/csv",
        use_container_width=True,
    )

    # Quick preview
    with st.expander("Preview report summary"):
        st.json({
            "total_transactions": report_data["total_transactions"],
            "critical_count": report_data["critical_count"],
            "fraud_rate_pct": report_data["fraud_rate_pct"],
            "top_bins_preview": report_data["top_bins"][:3],
            "top_ips_preview": report_data["top_ips_card_testing"][:3],
        })


# ── Main app ──────────────────────────────────────────────────────────────────

def main():
    st.title("🛡️ Solaris Media — Fraud Detection Dashboard")
    st.caption("Card-testing & subscription fraud detection · Real-time risk scoring")

    if not check_data_exists():
        st.error(
            f"Enriched dataset not found at `{DATA_PATH}`.\n\n"
            "Run the pipeline first:\n"
            "```bash\n"
            "python data/generate_dataset.py\n"
            "python pipeline/run_pipeline.py\n"
            "```"
        )
        st.stop()

    df = load_data()
    filtered = render_sidebar(df)

    if filtered.empty:
        st.warning("No transactions match the current filters.")
        st.stop()

    st.markdown("---")
    render_kpis(filtered)

    st.markdown("---")
    render_time_series(filtered)

    st.markdown("---")
    render_risk_distribution(filtered)

    st.markdown("---")
    render_anomalies_table(filtered)

    st.markdown("---")
    render_pattern_insights(filtered)

    st.markdown("---")
    render_export_section(filtered)

    st.markdown("---")
    st.caption("Solaris Media · Fraud Intelligence Platform · Powered by Streamlit + Plotly")


if __name__ == "__main__":
    main()
