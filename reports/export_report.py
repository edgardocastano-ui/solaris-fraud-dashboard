"""
Generate fraud pattern report in CSV and JSON formats.
Usage: python reports/export_report.py
"""

import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
REPORTS_DIR = os.path.dirname(os.path.abspath(__file__))
ENRICHED_PATH = os.path.join(DATA_DIR, "enriched_transactions.csv")
CSV_OUT = os.path.join(REPORTS_DIR, "fraud_pattern_report.csv")
JSON_OUT = os.path.join(REPORTS_DIR, "fraud_pattern_report.json")

FOREIGN_PREFIXES = ("5.188.", "185.220.", "193.32.", "45.142.", "91.108.")
DECLINE_STATUSES = {"declined_fraud", "declined_insufficient_funds"}


def build_top_bins(df: pd.DataFrame):
    stats = (
        df.groupby("card_bin")
        .agg(
            total_txns=("transaction_id", "count"),
            avg_risk_score=("risk_score", "mean"),
            declines=("status", lambda s: s.isin(DECLINE_STATUSES).sum()),
            chargebacks=("status", lambda s: (s == "chargeback").sum()),
            approvals=("status", lambda s: (s == "approved").sum()),
        )
        .reset_index()
    )
    stats["decline_rate_pct"] = (stats["declines"] / stats["total_txns"] * 100).round(1)
    stats["avg_risk_score"] = stats["avg_risk_score"].round(1)
    return stats.nlargest(10, "avg_risk_score").to_dict("records")


def build_top_ips(df: pd.DataFrame):
    stats = (
        df.groupby("ip_address")
        .agg(
            unique_cards=("card_last4", "nunique"),
            unique_bins=("card_bin", "nunique"),
            total_txns=("transaction_id", "count"),
            avg_risk_score=("risk_score", "mean"),
            declines=("status", lambda s: s.isin(DECLINE_STATUSES).sum()),
        )
        .reset_index()
    )
    stats["avg_risk_score"] = stats["avg_risk_score"].round(1)
    stats["is_known_fraud_ip"] = stats["ip_address"].str.startswith(FOREIGN_PREFIXES)
    return stats.nlargest(15, "unique_cards").to_dict("records")


def build_time_patterns(df: pd.DataFrame):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["hour"] = df["timestamp"].dt.hour
    df["dow"] = df["timestamp"].dt.day_name()
    hourly = (
        df.groupby("hour")
        .agg(
            count=("transaction_id", "count"),
            avg_risk=("risk_score", "mean"),
            critical_count=("risk_level", lambda x: (x == "CRITICAL").sum()),
        )
        .reset_index()
    )
    hourly["avg_risk"] = hourly["avg_risk"].round(1)
    return hourly.to_dict("records")


def build_geo_anomalies(df: pd.DataFrame):
    geo = df[
        df["ip_address"].str.startswith(FOREIGN_PREFIXES) | (df["bin_country"] != df["country"])
    ]
    summary = (
        geo.groupby(["country", "bin_country"])
        .agg(
            count=("transaction_id", "count"),
            avg_risk=("risk_score", "mean"),
        )
        .reset_index()
        .sort_values("count", ascending=False)
    )
    summary["avg_risk"] = summary["avg_risk"].round(1)
    return summary.to_dict("records")


def build_report(df: pd.DataFrame) -> dict:
    return {
        "meta": {
            "generated_at": pd.Timestamp.now().isoformat(),
            "total_transactions": len(df),
            "date_range": {
                "from": df["timestamp"].min() if not df.empty else None,
                "to": df["timestamp"].max() if not df.empty else None,
            },
        },
        "summary": {
            "total_transactions": len(df),
            "critical_count": int((df["risk_level"] == "CRITICAL").sum()),
            "high_count": int((df["risk_level"] == "HIGH").sum()),
            "medium_count": int((df["risk_level"] == "MEDIUM").sum()),
            "low_count": int((df["risk_level"] == "LOW").sum()),
            "fraud_rate_pct": round(
                df["status"].isin(["declined_fraud", "chargeback"]).mean() * 100, 2
            ),
            "chargeback_count": int((df["status"] == "chargeback").sum()),
            "avg_risk_score": round(df["risk_score"].mean(), 1),
        },
        "top_bins": build_top_bins(df),
        "top_ips_card_testing": build_top_ips(df),
        "time_patterns": build_time_patterns(df),
        "geo_anomalies": build_geo_anomalies(df),
    }


def flatten_to_csv_rows(report: dict):
    """Flatten report sections into rows for CSV export."""
    rows = []
    summary = report["summary"]

    # Section: top BINs
    for item in report["top_bins"]:
        rows.append({
            "section": "top_bins",
            "key": item["card_bin"],
            "metric_1_name": "total_txns",
            "metric_1_value": item["total_txns"],
            "metric_2_name": "avg_risk_score",
            "metric_2_value": item["avg_risk_score"],
            "metric_3_name": "decline_rate_pct",
            "metric_3_value": item["decline_rate_pct"],
        })

    # Section: top IPs
    for item in report["top_ips_card_testing"]:
        rows.append({
            "section": "top_ips",
            "key": item["ip_address"],
            "metric_1_name": "unique_cards",
            "metric_1_value": item["unique_cards"],
            "metric_2_name": "total_txns",
            "metric_2_value": item["total_txns"],
            "metric_3_name": "avg_risk_score",
            "metric_3_value": item["avg_risk_score"],
        })

    # Section: geo anomalies
    for item in report["geo_anomalies"]:
        rows.append({
            "section": "geo_anomalies",
            "key": f"{item['country']}_vs_{item['bin_country']}",
            "metric_1_name": "count",
            "metric_1_value": item["count"],
            "metric_2_name": "avg_risk",
            "metric_2_value": item["avg_risk"],
            "metric_3_name": "",
            "metric_3_value": "",
        })

    # Section: time patterns (top 5 riskiest hours)
    time_sorted = sorted(report["time_patterns"], key=lambda x: -x["avg_risk"])
    for item in time_sorted[:5]:
        rows.append({
            "section": "riskiest_hours",
            "key": f"hour_{item['hour']:02d}",
            "metric_1_name": "count",
            "metric_1_value": item["count"],
            "metric_2_name": "avg_risk",
            "metric_2_value": item["avg_risk"],
            "metric_3_name": "critical_count",
            "metric_3_value": item["critical_count"],
        })

    return rows


def run():
    if not os.path.exists(ENRICHED_PATH):
        print(f"ERROR: {ENRICHED_PATH} not found.")
        print("Run: python data/generate_dataset.py && python pipeline/run_pipeline.py")
        sys.exit(1)

    print(f"Loading {ENRICHED_PATH} ...")
    df = pd.read_csv(ENRICHED_PATH)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    print(f"  {len(df)} transactions loaded")

    report = build_report(df)

    # Write JSON
    with open(JSON_OUT, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"JSON report → {JSON_OUT}")

    # Write CSV
    csv_rows = flatten_to_csv_rows(report)
    pd.DataFrame(csv_rows).to_csv(CSV_OUT, index=False)
    print(f"CSV report  → {CSV_OUT}")

    # Print summary
    s = report["summary"]
    print(f"\nSummary:")
    print(f"  Total transactions : {s['total_transactions']:,}")
    print(f"  CRITICAL           : {s['critical_count']:,}")
    print(f"  HIGH               : {s['high_count']:,}")
    print(f"  Fraud rate         : {s['fraud_rate_pct']}%")
    print(f"  Avg risk score     : {s['avg_risk_score']}")


if __name__ == "__main__":
    run()
