# Solaris Media — Fraud Detection Dashboard

Real-time fraud detection system for subscription card-testing attacks on Solaris Media's
monthly/annual plan upgrades. Detects 5 fraud patterns using behavioral signals and risk scoring,
visualized in an interactive Streamlit dashboard.

---

## Quick Start

```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate       # macOS/Linux
# .venv\Scripts\activate        # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate synthetic dataset
python data/generate_dataset.py

# 4. Run risk scoring pipeline
python pipeline/run_pipeline.py

# 5. Launch dashboard
streamlit run dashboard/app.py
```

Dashboard opens at **http://localhost:8501**

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data generation | Python stdlib (`random`, `csv`) |
| Analytical queries | **DuckDB** — SQL over CSV, no server required |
| Data manipulation | Pandas |
| Visualization | Plotly (interactive charts) |
| Dashboard | Streamlit |
| Numerics | NumPy |

---

## Project Structure

```
solaris-fraud-dashboard/
├── data/
│   ├── generate_dataset.py        # Generates transactions.csv (650 rows, 30 days)
│   ├── transactions.csv           # Raw synthetic transactions
│   └── enriched_transactions.csv  # With risk_score, risk_level, signals_triggered
├── pipeline/
│   ├── __init__.py
│   ├── risk_scoring.py            # 5 fraud signals → risk score → LOW/MEDIUM/HIGH/CRITICAL
│   └── run_pipeline.py            # Entry point: reads CSV → scores → writes enriched CSV
├── dashboard/
│   └── app.py                     # Streamlit app (6 sections + sidebar filters)
├── reports/
│   ├── export_report.py           # Generates fraud_pattern_report.csv + .json
│   ├── fraud_pattern_report.csv   # Top BINs, IPs, time patterns, geo anomalies
│   └── fraud_pattern_report.json  # Same content, structured for API consumption
├── screenshots/                   # Dashboard captures
├── requirements.txt
└── README.md
```

---

## Fraud Detection Logic

### The Problem

Solaris Media's subscription model is vulnerable to **card-testing attacks**: fraudsters use
automated scripts to test stolen card numbers against the $4.99/month plan. Valid cards are
immediately upgraded to the $49.99/year plan before the chargeback window closes.

A single fraud ring can test dozens of cards per hour from the same IP, leaving a detectable
trail in transaction logs.

### 5 Fraud Signals

#### Signal 1 — IP Velocity (+40 points)
**What it detects:** A single IP address testing multiple different cards in a short window.

**Logic:** If more than 3 unique cards are observed from the same IP within any rolling 24-hour
window, every transaction from that IP in that window is flagged.

**Why it matters:** Legitimate users own 1–2 cards. Seeing 5–8 distinct cards from the same IP
in under 2 hours is a near-certain indicator of automated card enumeration. This signal carries
the highest weight (+40) because it has very low false-positive rate.

---

#### Signal 2 — Rapid Tier Upgrade (+30 points)
**What it detects:** A customer who upgrades from monthly ($4.99) to annual ($49.99) within 24 hours.

**Logic:** If the same `customer_email` has an approved `monthly` transaction followed by an
`annual` transaction within 24 hours, both transactions are flagged.

**Why it matters:** Organic annual upgrades happen after days or weeks of using the product.
A sub-24h upgrade—especially when combined with other signals—suggests the attacker confirmed
the card works (monthly approval) and immediately maximized the charge (annual upgrade) before
the issuer notices.

---

#### Signal 3 — BIN Decline Rate (+25 points)
**What it detects:** Card BIN prefixes with an abnormally high decline rate, indicating a batch
of compromised or synthetic cards sharing the same issuer range.

**Logic:** For each BIN (first 6 digits), calculate `declined / total` across all transactions.
If the rate exceeds 40% (minimum 3 transactions), every transaction using that BIN is flagged.

**Why it matters:** Legitimate BINs typically have <15% decline rates. Stolen card batches from
the same breach often share BIN prefixes. A BIN with >40% declines signals that the issuer has
already flagged many cards in that range.

**DuckDB query used:**
```sql
SELECT card_bin,
       COUNT(*) AS total,
       SUM(CASE WHEN status IN ('declined_fraud','declined_insufficient_funds')
                THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS decline_rate_pct
FROM transactions
GROUP BY card_bin
HAVING COUNT(*) >= 3 AND decline_rate_pct > 40
```

---

#### Signal 4 — Geo Mismatch (+20 points)
**What it detects:** Transactions where the card's issuing country doesn't match the IP's
geographic origin.

**Logic:** Two conditions trigger this signal:
1. The `ip_address` starts with a known high-fraud IP prefix (e.g., `185.220.x.x` — Tor exit
   nodes and bulletproof hosting ranges)
2. The `bin_country` field differs from the `country` field (BIN issued in Brazil, IP from
   Eastern Europe)

**Why it matters:** While cross-border purchases are normal, a Brazilian card used from a
known anonymization network in Eastern Europe is a strong fraud indicator. This signal is
weighted lower (+20) because it can occasionally fire for legitimate VPN users.

---

#### Signal 5 — Repeated Failures Before Approval (+25 points)
**What it detects:** A card that fails 3 or more times before finally getting approved—a pattern
consistent with automated retries or card-stuffing attacks.

**Logic:** For each unique card (`card_bin + card_last4`), scan the transaction history in
chronological order. If 3+ consecutive declines are followed by an approval, flag all
transactions in that sequence.

**Why it matters:** Legitimate cardholders rarely retry the same card 3+ times. This pattern
typically represents either an automated attack trying different parameters (amount, currency)
until one succeeds, or a fraudster testing whether a declined card was blocked or just had
insufficient funds.

---

### Risk Score Composition

Signals are **additive**. A transaction can trigger multiple signals simultaneously:

```
risk_score = (ip_velocity × 40) + (rapid_upgrade × 30) + (bin_decline × 25)
           + (geo_mismatch × 20) + (repeated_failures × 25)
```

**Maximum possible score: 140** (all 5 signals active)

| Score | Level | Meaning |
|-------|-------|---------|
| 0–20 | LOW | Normal transaction, no signals |
| 21–40 | MEDIUM | One weak signal (e.g., geo mismatch alone) |
| 41–65 | HIGH | One strong signal or two weak signals |
| 66+ | **CRITICAL** | Multiple signals — likely active fraud |

**Example CRITICAL transaction (score 85):**
- IP Velocity: `185.220.101.88` tested 7 cards in 90 minutes (+40)
- BIN Decline Rate: BIN `999003` has 67% decline rate across 9 transactions (+25)
- Geo Mismatch: Foreign IP prefix on a Brazilian card (+20)
- **Total: 85 → CRITICAL**

---

## Dashboard Navigation Guide

### Sidebar — Start Here

The sidebar controls what data every section displays. Apply filters before reading any chart.

| Filter | Recommended use |
|--------|----------------|
| **Date range** | Narrow to a specific incident window — e.g., last 7 days if investigating a recent spike |
| **Country** | Isolate Brazil (BRL) to check if fraud concentrates in one market |
| **Subscription Tier** | Filter to `annual` only to find rapid-upgrade victims |
| **Risk Level** | Select `CRITICAL` + `HIGH` only for focused fraud review |
| **Status** | Select `declined_fraud` + `chargeback` to see confirmed fraud only |
| **Export Filtered CSV** | Downloads the currently visible transactions for offline analysis |

---

### Section 1 — KPI Cards

Five top-level metrics for the current filter selection.

- **Fraud Rate %** — Ratio of `declined_fraud + chargeback` transactions. Healthy baseline
  is <5%. If this exceeds 15%, investigate urgently.
- **CRITICAL count** — Number of transactions with score ≥ 66. Any non-zero value requires
  immediate review.
- **Avg Risk Score** — Dataset-wide average. Normal is <10. During an active attack this
  spikes above 20.

---

### Section 2 — Time Series (Volume + Fraud Score)

**Stacked bars by risk level + Avg Risk Score line (secondary axis)**

Look for:
1. **Days where CRITICAL (purple) or HIGH (red) bars appear** — these are attack windows
2. **Spikes in the Avg Risk Score line** that don't correspond to volume spikes — small
   number of very suspicious transactions, not a volume anomaly
3. **Correlation between volume spikes and score spikes** — a volume spike with flat score
   is organic traffic; a score spike with flat volume is targeted fraud

---

### Section 3 — Risk Distribution

- **Donut chart** — Quick visual of LOW/MEDIUM/HIGH/CRITICAL proportions. Healthy datasets
  are >85% LOW.
- **Stacked bar by country** — Identifies which market is under attack. If one country shows
  disproportionate CRITICAL volume, that's your primary target.

---

### Section 4 — Top 20 Anomalies Table

The highest-risk transactions in the current filter. **CRITICAL rows are highlighted in
dark purple, HIGH in dark red.**

Key columns to read:
- `signals_triggered` — The comma-separated list explains *why* this transaction scored high.
  A transaction with `ip_velocity, bin_decline_rate, geo_mismatch` is a textbook card-testing hit.
- `risk_score` — Sort mentally: 85 = 3 signals, 65 = 2 signals, 40 = 1 signal.
- `customer_email` — Check if the same email appears multiple times with different cards.

---

### Section 5 — Pattern Insights (4 Tabs)

#### Tab: BINs
Shows which card BIN prefixes are generating the most risk.

- **Bar chart** — Top 10 BINs by avg risk score, colored by decline rate. Dark red bars = bad BINs.
- **Detail table** — Sort by `Decline Rate %` to find BINs to block at the payment processor level.
  Any BIN above 40% decline rate should be added to a block list.

#### Tab: IPs
Shows which IP addresses are testing multiple cards (card-testing map).

- **Bar chart** — Top 10 IPs by unique card count. Any bar above 3 cards is suspicious.
- **Suspicious IPs table** — IPs with >3 unique cards. Share this list with your infrastructure
  team to add to firewall rules.

#### Tab: Horario
Fraud concentration by hour and day of week.

- **Risk heatmap** (top) — Dark red cells = hours with highest avg fraud score. Card-testing
  bots often run overnight (00:00–06:00) to avoid manual review.
- **Volume heatmap** (bottom) — Compare against risk heatmap. If volume is low but risk is
  high at 3am, that's automated attack traffic.

#### Tab: Geo
Transactions where BIN country ≠ IP country.

- **Metric** — Total mismatch count. Should be <5% of transactions.
- **Summary table** — Shows which country pairs are mismatching. `BR → foreign` combos
  indicate Brazilian cards being used by overseas attackers.

---

### Section 6 — Export Report

Two download buttons:

- **`fraud_pattern_report.json`** — Structured report with top BINs, top IPs, hourly patterns,
  and geo anomalies. Suitable for ingestion by risk management APIs or alerting systems.
- **`fraud_pattern_report.csv`** — Flat version of the same data for spreadsheet analysis or
  sharing with the fraud ops team.

---

## Pattern Insights Report

The report (`reports/fraud_pattern_report.json`) is also generated as a standalone file:

```bash
python reports/export_report.py
```

Structure:
```json
{
  "meta": { "generated_at": "...", "total_transactions": 650 },
  "summary": {
    "critical_count": 10,
    "fraud_rate_pct": 22.15,
    "avg_risk_score": 9.7
  },
  "top_bins": [...],
  "top_ips_card_testing": [...],
  "time_patterns": [...],
  "geo_anomalies": [...]
}
```

---

## Dataset Details

| Property | Value |
|----------|-------|
| Total transactions | 650 |
| Date range | 30 days |
| Currencies | BRL (Brazil), MXN (Mexico), COP (Colombia) |
| Subscription tiers | Monthly, Annual |
| Fraud patterns embedded | 6 (card testing, bad BINs, rapid upgrades, repeated failures, geo mismatches, chargeback clusters) |
| CRITICAL transactions | 10 (score ≥ 66) |
| HIGH transactions | 32 (score 41–65) |
