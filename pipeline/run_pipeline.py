"""
Pipeline entry point: reads transactions.csv → enriches → saves enriched_transactions.csv

DuckDB is used to:
  - Read the CSV efficiently with read_csv_auto
  - Pre-compute BIN decline rates and IP velocity counts via SQL
  - Write the enriched output back to CSV
"""

import os
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.risk_scoring import score_transactions

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
INPUT_PATH = os.path.join(DATA_DIR, "transactions.csv")
OUTPUT_PATH = os.path.join(DATA_DIR, "enriched_transactions.csv")


def load_with_duckdb(path: str) -> pd.DataFrame:
    """Read CSV with DuckDB and return a pandas DataFrame."""
    conn = duckdb.connect()
    df = conn.execute(f"SELECT * FROM read_csv_auto('{path}')").df()
    conn.close()
    return df


def compute_bin_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use DuckDB SQL to compute per-BIN decline rates and risk aggregates.
    Returns a summary DataFrame for reporting.
    """
    conn = duckdb.connect()
    conn.register("txns", df)
    bin_stats = conn.execute("""
        SELECT
            card_bin,
            COUNT(*)                                                              AS total_txns,
            SUM(CASE WHEN status IN ('declined_fraud', 'declined_insufficient_funds')
                     THEN 1 ELSE 0 END)                                          AS total_declines,
            SUM(CASE WHEN status = 'chargeback' THEN 1 ELSE 0 END)              AS total_chargebacks,
            ROUND(
                CAST(SUM(CASE WHEN status IN ('declined_fraud', 'declined_insufficient_funds')
                              THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100,
                1
            )                                                                    AS decline_rate_pct,
            ROUND(AVG(
                CASE country
                    WHEN 'BR' THEN amount
                    ELSE amount
                END
            ), 2)                                                                AS avg_amount
        FROM txns
        GROUP BY card_bin
        HAVING COUNT(*) >= 3
        ORDER BY decline_rate_pct DESC
    """).df()
    conn.close()
    return bin_stats


def compute_ip_velocity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Use DuckDB SQL to find IPs with high card diversity (card-testing indicator).
    """
    conn = duckdb.connect()
    conn.register("txns", df)
    ip_stats = conn.execute("""
        SELECT
            ip_address,
            COUNT(*)                                           AS total_txns,
            COUNT(DISTINCT card_last4)                        AS unique_cards,
            COUNT(DISTINCT card_bin)                          AS unique_bins,
            SUM(CASE WHEN status IN ('declined_fraud', 'declined_insufficient_funds')
                     THEN 1 ELSE 0 END)                       AS total_declines,
            ROUND(
                CAST(SUM(CASE WHEN status IN ('declined_fraud', 'declined_insufficient_funds')
                              THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100,
                1
            )                                                  AS decline_rate_pct
        FROM txns
        GROUP BY ip_address
        ORDER BY unique_cards DESC
        LIMIT 20
    """).df()
    conn.close()
    return ip_stats


def save_enriched_duckdb(enriched: pd.DataFrame, path: str) -> None:
    """Write enriched DataFrame to CSV via DuckDB COPY."""
    conn = duckdb.connect()
    conn.register("enriched", enriched)
    conn.execute(f"COPY enriched TO '{path}' (HEADER, DELIMITER ',')")
    conn.close()


def run():
    print(f"[DuckDB] Reading {INPUT_PATH} ...")
    df = load_with_duckdb(INPUT_PATH)
    print(f"  Loaded {len(df)} transactions")

    print("[DuckDB] Pre-computing BIN stats ...")
    bin_stats = compute_bin_stats(df)
    print(f"  {len(bin_stats)} BINs analysed")
    bad_bins = bin_stats[bin_stats["decline_rate_pct"] > 40]["card_bin"].tolist()
    print(f"  Bad BINs (>40% decline): {bad_bins}")

    print("[DuckDB] Pre-computing IP velocity ...")
    ip_stats = compute_ip_velocity(df)
    suspicious_ips = ip_stats[ip_stats["unique_cards"] > 3]["ip_address"].tolist()
    print(f"  Suspicious IPs (>3 unique cards): {len(suspicious_ips)} found")

    print("[pandas] Scoring transactions ...")
    enriched = score_transactions(df)

    print(f"[DuckDB] Writing enriched dataset → {OUTPUT_PATH}")
    save_enriched_duckdb(enriched, OUTPUT_PATH)

    # Summary via DuckDB query on enriched data
    conn = duckdb.connect()
    conn.register("enriched", enriched)
    summary = conn.execute("""
        SELECT
            risk_level,
            COUNT(*)                    AS count,
            ROUND(AVG(risk_score), 1)   AS avg_score,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
        FROM enriched
        GROUP BY risk_level
        ORDER BY avg_score DESC
    """).df()
    conn.close()

    print("\nRisk level distribution (via DuckDB):")
    for _, row in summary.iterrows():
        print(f"  {row['risk_level']:10s}: {int(row['count']):4d}  ({row['pct']:.1f}%)")

    critical = enriched[enriched["risk_level"] == "CRITICAL"]
    print(f"\nCRITICAL transactions ({len(critical)}):")
    for _, row in critical.head(5).iterrows():
        print(f"  {row['transaction_id']} | {row['customer_email']} | score={row['risk_score']} | signals={row['signals_triggered']}")

    print(f"\nDone! Enriched dataset saved to {OUTPUT_PATH}")


if __name__ == "__main__":
    run()
