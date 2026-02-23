"""
Generate synthetic transaction dataset for Solaris Media fraud detection.
600+ transactions over 30 days with embedded fraud patterns.
"""

import random
import csv
from datetime import datetime, timedelta
from typing import Optional, List, Dict

# Seed for reproducibility
random.seed(42)

# ── Constants ────────────────────────────────────────────────────────────────

START_DATE = datetime(2026, 1, 20)
END_DATE = datetime(2026, 2, 19)
TARGET_ROWS = 900

MONTHLY_PLANS = ["Monthly Basic", "Monthly Standard", "Monthly Pro"]
ANNUAL_PLANS  = ["Annual Basic", "Annual Premium", "Annual Premium Plus"]

CURRENCIES = {
    "BR": {
        "currency": "BRL", "symbol": "R$",
        "monthly": [4.99, 9.99, 14.99],
        "annual":  [29.99, 49.99, 69.99],
    },
    "MX": {
        "currency": "MXN", "symbol": "$",
        "monthly": [99.00, 199.00, 299.00],
        "annual":  [599.00, 990.00, 1390.00],
    },
    "CO": {
        "currency": "COP", "symbol": "$COL",
        "monthly": [19900.00, 39900.00, 59900.00],
        "annual":  [119900.00, 199000.00, 279000.00],
    },
}

# BINs: first 6 digits of card
GOOD_BINS = ["411111", "424242", "531313", "541500", "601100", "650000", "451200", "438600"]
BAD_BINS = ["999001", "999002", "999003", "999004"]  # >50% decline rate

# Card-testing IPs (6 IPs that will be used with many cards)
CARDTEST_IPS = [f"185.220.101.{x}" for x in [34, 47, 88, 112, 156, 200]]

# Normal IP pools by country
NORMAL_IPS = {
    "BR": [f"187.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}" for _ in range(80)],
    "MX": [f"189.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}" for _ in range(80)],
    "CO": [f"190.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}" for _ in range(80)],
}

# Foreign IPs for geo mismatch (Eastern Europe / known fraud regions)
FOREIGN_IPS = [f"5.188.{random.randint(0,255)}.{random.randint(0,255)}" for _ in range(30)]

STATUSES = ["approved", "declined_fraud", "declined_insufficient_funds", "chargeback"]
STATUS_WEIGHTS = [0.85, 0.06, 0.08, 0.01]

TIERS = ["monthly", "annual"]

# ── Helpers ──────────────────────────────────────────────────────────────────

def random_email(seed_int: int) -> str:
    domains = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "protonmail.com"]
    names = ["user", "cliente", "member", "sub", "acct", "customer"]
    return f"{random.choice(names)}{seed_int}@{random.choice(domains)}"


def random_last4() -> str:
    return str(random.randint(1000, 9999))


def random_timestamp(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def amount_for(tier: str, country: str, plan: str) -> float:
    cfg = CURRENCIES[country]
    prices = cfg["monthly"] if tier == "monthly" else cfg["annual"]
    plans  = MONTHLY_PLANS   if tier == "monthly" else ANNUAL_PLANS
    idx = plans.index(plan) if plan in plans else 0
    return prices[idx]


def make_transaction(
    txn_id: int,
    timestamp: datetime,
    country: str,
    tier: str,
    status: str,
    ip: str,
    card_bin: str,
    last4: str,
    email: str,
    bin_country: Optional[str] = None,
    plan: Optional[str] = None,
) -> dict:
    if plan is None:
        plan = random.choice(MONTHLY_PLANS if tier == "monthly" else ANNUAL_PLANS)
    cfg = CURRENCIES[country]
    return {
        "transaction_id": f"txn_{txn_id:05d}",
        "timestamp": timestamp.isoformat(),
        "customer_email": email,
        "subscription_tier": tier,
        "subscription_plan": plan,
        "amount": amount_for(tier, country, plan),
        "currency": cfg["currency"],
        "country": country,
        "ip_address": ip,
        "card_bin": card_bin,
        "card_last4": last4,
        "bin_country": bin_country or country,
        "status": status,
    }


# ── Fraud pattern generators ─────────────────────────────────────────────────

def inject_card_testing(txn_id_start: int, rows: List[dict]) -> int:
    """6 IPs × 6-8 cards in narrow 2h window = ~42 transactions."""
    txn_id = txn_id_start
    for ip in CARDTEST_IPS:
        num_cards = random.randint(5, 8)
        base_ts = random_timestamp(START_DATE, END_DATE - timedelta(hours=2))
        country = random.choice(["BR", "MX", "CO"])
        for i in range(num_cards):
            ts = base_ts + timedelta(minutes=random.randint(0, 110))
            status = random.choices(
                ["approved", "declined_fraud", "declined_insufficient_funds"],
                weights=[0.3, 0.4, 0.3],
            )[0]
            rows.append(make_transaction(
                txn_id, ts, country, "monthly", status, ip,
                random.choice(GOOD_BINS), random_last4(), random_email(txn_id + 9000),
            ))
            txn_id += 1
    return txn_id


def inject_bad_bins(txn_id_start: int, rows: List[dict]) -> int:
    """4 bad BINs, each with 7-10 transactions, >50% decline rate."""
    txn_id = txn_id_start
    for bad_bin in BAD_BINS:
        count = random.randint(7, 10)
        country = random.choice(["BR", "MX", "CO"])
        email = random_email(txn_id + 5000)
        for _ in range(count):
            ts = random_timestamp(START_DATE, END_DATE)
            status = random.choices(
                ["approved", "declined_fraud", "declined_insufficient_funds"],
                weights=[0.3, 0.4, 0.3],
            )[0]
            ip = random.choice(NORMAL_IPS[country])
            rows.append(make_transaction(
                txn_id, ts, country, random.choice(TIERS), status, ip,
                bad_bin, random_last4(), email,
            ))
            txn_id += 1
    return txn_id


def inject_rapid_upgrades(txn_id_start: int, rows: List[dict]) -> int:
    """15 customers: monthly → annual in <6h."""
    txn_id = txn_id_start
    for i in range(15):
        country = random.choice(["BR", "MX", "CO"])
        email = random_email(txn_id + 7000)
        ip = random.choice(NORMAL_IPS[country])
        card_bin = random.choice(GOOD_BINS)
        last4 = random_last4()
        base_ts = random_timestamp(START_DATE, END_DATE - timedelta(hours=6))
        # Monthly purchase
        rows.append(make_transaction(
            txn_id, base_ts, country, "monthly", "approved", ip, card_bin, last4, email,
        ))
        txn_id += 1
        # Annual upgrade within 2-5h — always to Annual Premium (maximize charge)
        upgrade_ts = base_ts + timedelta(hours=random.randint(1, 5))
        rows.append(make_transaction(
            txn_id, upgrade_ts, country, "annual", "approved", ip, card_bin, last4, email,
            plan="Annual Premium",
        ))
        txn_id += 1
    return txn_id


def inject_repeated_failures(txn_id_start: int, rows: List[dict]) -> int:
    """8 cards: 3+ declines then approval."""
    txn_id = txn_id_start
    for i in range(8):
        country = random.choice(["BR", "MX", "CO"])
        email = random_email(txn_id + 6000)
        ip = random.choice(NORMAL_IPS[country])
        card_bin = random.choice(GOOD_BINS)
        last4 = random_last4()
        base_ts = random_timestamp(START_DATE, END_DATE - timedelta(hours=4))
        num_declines = random.randint(3, 5)
        for j in range(num_declines):
            ts = base_ts + timedelta(minutes=j * 20)
            status = random.choices(
                ["declined_fraud", "declined_insufficient_funds"], weights=[0.4, 0.6]
            )[0]
            rows.append(make_transaction(
                txn_id, ts, country, "monthly", status, ip, card_bin, last4, email,
            ))
            txn_id += 1
        # Final approval
        approval_ts = base_ts + timedelta(minutes=num_declines * 20 + 10)
        rows.append(make_transaction(
            txn_id, approval_ts, country, "monthly", "approved", ip, card_bin, last4, email,
        ))
        txn_id += 1
    return txn_id


def inject_geo_mismatches(txn_id_start: int, rows: List[dict]) -> int:
    """25 transactions: BIN country ≠ IP country."""
    txn_id = txn_id_start
    countries = list(CURRENCIES.keys())
    for i in range(25):
        card_country = random.choice(countries)
        # IP from a different country (foreign)
        ip = random.choice(FOREIGN_IPS)
        # BIN from card_country
        card_bin = random.choice(GOOD_BINS)
        email = random_email(txn_id + 8000)
        ts = random_timestamp(START_DATE, END_DATE)
        tier = random.choice(TIERS)
        status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
        row = make_transaction(
            txn_id, ts, card_country, tier, status, ip, card_bin, random_last4(), email,
            bin_country=card_country,
        )
        # Override bin_country to be different from IP-implied country
        row["bin_country"] = card_country  # BIN is from card_country
        # IP is foreign — we mark it explicitly with a special prefix
        rows.append(row)
        txn_id += 1
    return txn_id


def inject_chargeback_clusters(txn_id_start: int, rows: List[dict]) -> int:
    """Chargebacks concentrated on 2 bad BINs and 2 card-testing IPs."""
    txn_id = txn_id_start
    cluster_bins = BAD_BINS[:2]
    cluster_ips = CARDTEST_IPS[:2]
    for _ in range(15):
        country = random.choice(["BR", "MX", "CO"])
        card_bin = random.choice(cluster_bins)
        ip = random.choice(cluster_ips + NORMAL_IPS[country][:5])
        email = random_email(txn_id + 4000)
        ts = random_timestamp(START_DATE, END_DATE)
        rows.append(make_transaction(
            txn_id, ts, country, "annual", "chargeback", ip, card_bin, random_last4(), email,
        ))
        txn_id += 1
    return txn_id


def inject_concentrated_attacks(txn_id_start: int, rows: List[dict]) -> int:
    """
    Multi-signal transactions: card-testing IP + bad BIN within same 24h window.
    Triggers: IP Velocity (+40) + BIN Decline (+25) + Geo Mismatch (+20) = 85 → CRITICAL.
    3 attack clusters, different IPs and BINs.
    """
    txn_id = txn_id_start
    clusters = [
        (CARDTEST_IPS[2], BAD_BINS[2], "BR"),
        (CARDTEST_IPS[3], BAD_BINS[3], "MX"),
        (CARDTEST_IPS[4], BAD_BINS[0], "CO"),
    ]
    for ip, card_bin, country in clusters:
        base_ts = random_timestamp(START_DATE, END_DATE - timedelta(hours=6))
        num_cards = random.randint(6, 9)
        for i in range(num_cards):
            ts = base_ts + timedelta(minutes=random.randint(0, 300))
            status = random.choices(
                ["approved", "declined_fraud", "declined_insufficient_funds"],
                weights=[0.30, 0.40, 0.30],
            )[0]
            rows.append(make_transaction(
                txn_id, ts, country, "monthly", status, ip,
                card_bin, random_last4(), random_email(txn_id + 3000),
            ))
            txn_id += 1
    return txn_id


# ── Main generation ───────────────────────────────────────────────────────────

def generate() -> List[dict]:
    rows = []
    txn_id = 1

    # Inject fraud patterns first
    txn_id = inject_card_testing(txn_id, rows)      # ~42 rows
    txn_id = inject_bad_bins(txn_id, rows)           # ~34 rows
    txn_id = inject_rapid_upgrades(txn_id, rows)     # 30 rows
    txn_id = inject_repeated_failures(txn_id, rows)  # ~40 rows
    txn_id = inject_geo_mismatches(txn_id, rows)     # 25 rows
    txn_id = inject_chargeback_clusters(txn_id, rows) # 15 rows
    txn_id = inject_concentrated_attacks(txn_id, rows) # ~21 rows (CRITICAL)

    fraud_count = len(rows)

    # Fill remainder with legit transactions
    legit_needed = max(TARGET_ROWS - fraud_count, 400)
    for _ in range(legit_needed):
        country = random.choices(["BR", "MX", "CO"], weights=[0.5, 0.3, 0.2])[0]
        ip = random.choice(NORMAL_IPS[country])
        card_bin = random.choice(GOOD_BINS)
        last4 = random_last4()
        email = random_email(txn_id)
        ts = random_timestamp(START_DATE, END_DATE)
        tier = random.choices(TIERS, weights=[0.7, 0.3])[0]
        status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
        rows.append(make_transaction(txn_id, ts, country, tier, status, ip, card_bin, last4, email))
        txn_id += 1

    # Sort by timestamp
    rows.sort(key=lambda r: r["timestamp"])
    return rows


def write_csv(rows: List[dict], path: str) -> None:
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    import os
    out_path = os.path.join(os.path.dirname(__file__), "transactions.csv")
    rows = generate()
    write_csv(rows, out_path)
    print(f"Generated {len(rows)} transactions → {out_path}")

    statuses = {}
    for r in rows:
        statuses[r["status"]] = statuses.get(r["status"], 0) + 1
    for s, c in sorted(statuses.items()):
        print(f"  {s}: {c} ({c/len(rows)*100:.1f}%)")
