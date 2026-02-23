"""
Risk scoring engine for Solaris Media fraud detection.

5 signals:
  - IP Velocity        +40  (>3 unique cards from same IP in 24h)
  - Rapid Tier Upgrade +30  (monthly → annual for same email in <24h)
  - BIN Decline Rate   +25  (>40% declines for same BIN, min 3 txns)
  - Geo Mismatch       +20  (BIN country ≠ IP-implied country)
  - Repeated Failures  +25  (3+ declines on same card before approval)

Risk levels:
  0-20  → LOW
  21-40 → MEDIUM
  41-65 → HIGH
  66+   → CRITICAL
"""

import json
from datetime import datetime, timedelta

import pandas as pd

SCORE_IP_VELOCITY = 40
SCORE_RAPID_UPGRADE = 30
SCORE_BIN_DECLINE = 25
SCORE_GEO_MISMATCH = 20
SCORE_REPEATED_FAILURES = 25

IP_VELOCITY_THRESHOLD = 3          # unique cards per IP per 24h
IP_VELOCITY_WINDOW_HOURS = 24
RAPID_UPGRADE_WINDOW_HOURS = 24
BIN_DECLINE_THRESHOLD = 0.40       # 40%
BIN_DECLINE_MIN_TXNS = 3
REPEATED_FAILURE_THRESHOLD = 3     # declines before approval

DECLINE_STATUSES = {"declined_fraud", "declined_insufficient_funds"}

# Countries with known high-fraud IP prefixes (simplified inference)
FOREIGN_IP_PREFIXES = ("5.188.", "185.220.", "193.32.", "45.142.", "91.108.")


def _parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def _ip_velocity_signal(df: pd.DataFrame) -> pd.Series:
    """
    Flag transactions where the source IP had >3 unique cards within 24h of this txn.
    Returns a boolean Series.
    """
    df = df.sort_values("timestamp").reset_index(drop=True)
    flagged = pd.Series(False, index=df.index)

    # Build lookup: for each row, count unique cards from same IP in [ts-24h, ts]
    # Efficient: group by IP then use a rolling window approach
    for ip, group in df.groupby("ip_address"):
        group = group.sort_values("timestamp")
        idx_list = group.index.tolist()
        ts_list = group["timestamp"].tolist()
        card_list = (group["card_bin"].astype(str) + "-" + group["card_last4"].astype(str)).tolist()

        for i, (idx, ts, _card) in enumerate(zip(idx_list, ts_list, card_list)):
            window_start = ts - timedelta(hours=IP_VELOCITY_WINDOW_HOURS)
            unique_cards = set()
            for j in range(i + 1):
                if ts_list[j] >= window_start:
                    unique_cards.add(card_list[j])
            if len(unique_cards) > IP_VELOCITY_THRESHOLD:
                flagged[idx] = True

    return flagged


def _rapid_upgrade_signal(df: pd.DataFrame) -> pd.Series:
    """
    Flag annual transactions where the same email had a monthly txn in the past 24h.
    Also flag the preceding monthly txn.
    """
    df = df.sort_values("timestamp").reset_index(drop=True)
    flagged = pd.Series(False, index=df.index)

    annual = df[df["subscription_tier"] == "annual"]
    monthly = df[df["subscription_tier"] == "monthly"]

    for _, ann_row in annual.iterrows():
        window_start = ann_row["timestamp"] - timedelta(hours=RAPID_UPGRADE_WINDOW_HOURS)
        prev_monthly = monthly[
            (monthly["customer_email"] == ann_row["customer_email"])
            & (monthly["timestamp"] >= window_start)
            & (monthly["timestamp"] < ann_row["timestamp"])
        ]
        if not prev_monthly.empty:
            flagged[ann_row.name] = True
            flagged[prev_monthly.index] = True

    return flagged


def _bin_decline_signal(df: pd.DataFrame) -> pd.Series:
    """
    Flag all transactions where the BIN has >40% decline rate (min 3 txns).
    """
    decline_mask = df["status"].isin(DECLINE_STATUSES)
    bin_stats = df.groupby("card_bin").agg(
        total=("transaction_id", "count"),
        declines=("status", lambda s: s.isin(DECLINE_STATUSES).sum()),
    )
    bad_bins = bin_stats[
        (bin_stats["total"] >= BIN_DECLINE_MIN_TXNS)
        & (bin_stats["declines"] / bin_stats["total"] > BIN_DECLINE_THRESHOLD)
    ].index

    return df["card_bin"].isin(bad_bins)


def _geo_mismatch_signal(df: pd.DataFrame) -> pd.Series:
    """
    Flag transactions where the IP suggests a foreign origin vs. the BIN country.
    Uses: bin_country field vs. IP prefix heuristic.
    """
    ip_is_foreign = df["ip_address"].str.startswith(FOREIGN_IP_PREFIXES)
    # Also flag when bin_country != country (explicit mismatch in dataset)
    bin_country_mismatch = df["bin_country"] != df["country"]
    return ip_is_foreign | bin_country_mismatch


def _repeated_failures_signal(df: pd.DataFrame) -> pd.Series:
    """
    Flag the approval (and preceding declines) when a card had 3+ declines before approval.
    """
    df = df.sort_values("timestamp").reset_index(drop=True)
    flagged = pd.Series(False, index=df.index)

    card_key = df["card_bin"].astype(str) + "-" + df["card_last4"].astype(str)
    df = df.copy()
    df["_card_key"] = card_key

    for card, group in df.groupby("_card_key"):
        group = group.sort_values("timestamp")
        consecutive_declines = 0
        decline_indices = []
        for _, row in group.iterrows():
            if row["status"] in DECLINE_STATUSES:
                consecutive_declines += 1
                decline_indices.append(row.name)
            elif row["status"] == "approved":
                if consecutive_declines >= REPEATED_FAILURE_THRESHOLD:
                    flagged[row.name] = True
                    for idx in decline_indices:
                        flagged[idx] = True
                consecutive_declines = 0
                decline_indices = []

    return flagged


def _risk_level(score: int) -> str:
    if score <= 20:
        return "LOW"
    elif score <= 40:
        return "MEDIUM"
    elif score <= 65:
        return "HIGH"
    else:
        return "CRITICAL"


def score_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich a transactions DataFrame with risk_score, risk_level, signals_triggered.
    Returns enriched copy.
    """
    df = _parse_timestamps(df)

    # Compute individual signals
    sig_ip_velocity = _ip_velocity_signal(df)
    sig_rapid_upgrade = _rapid_upgrade_signal(df)
    sig_bin_decline = _bin_decline_signal(df)
    sig_geo_mismatch = _geo_mismatch_signal(df)
    sig_repeated_failures = _repeated_failures_signal(df)

    # Aggregate scores
    scores = (
        sig_ip_velocity.astype(int) * SCORE_IP_VELOCITY
        + sig_rapid_upgrade.astype(int) * SCORE_RAPID_UPGRADE
        + sig_bin_decline.astype(int) * SCORE_BIN_DECLINE
        + sig_geo_mismatch.astype(int) * SCORE_GEO_MISMATCH
        + sig_repeated_failures.astype(int) * SCORE_REPEATED_FAILURES
    )

    # Build signals_triggered JSON list per row
    def build_signals(row_idx: int) -> str:
        signals = []
        if sig_ip_velocity.iloc[row_idx]:
            signals.append("ip_velocity")
        if sig_rapid_upgrade.iloc[row_idx]:
            signals.append("rapid_tier_upgrade")
        if sig_bin_decline.iloc[row_idx]:
            signals.append("bin_decline_rate")
        if sig_geo_mismatch.iloc[row_idx]:
            signals.append("geo_mismatch")
        if sig_repeated_failures.iloc[row_idx]:
            signals.append("repeated_failures")
        return json.dumps(signals)

    result = df.copy()
    result["risk_score"] = scores.values
    result["risk_level"] = scores.map(_risk_level).values
    result["signals_triggered"] = [build_signals(i) for i in range(len(df))]

    return result
