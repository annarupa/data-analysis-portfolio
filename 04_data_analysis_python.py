"""
Data Quality Profiler & Anomaly Detection
==========================================
Use Case: Automated data profiling for production support —
          surfaces data quality issues, missing values, and
          statistical anomalies in daily data loads.

Covers two domains:
  1. Financial Services — incentive & partner transaction data
  2. Healthcare         — customer case SLA monitoring

Author: Anna Rupa Anthony
Tools:  Python, pandas, numpy, matplotlib
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


# ================================================================
# SECTION 1: FINANCIAL SERVICES — Incentive Transaction Analysis
# ================================================================

np.random.seed(42)
n = 500

fin_data = {
    "transaction_id":   [f"TXN{str(i).zfill(5)}" for i in range(1, n + 1)],
    "partner_id":       np.random.choice([f"P{str(i).zfill(3)}" for i in range(1, 51)], n),
    "program_code":     np.random.choice(["PROG_A", "PROG_B", "PROG_C", "PROG_D"], n),
    "transaction_date": [datetime.today() - timedelta(days=int(np.random.randint(0, 90))) for _ in range(n)],
    "settlement_date":  [datetime.today() - timedelta(days=int(np.random.randint(0, 85))) for _ in range(n)],
    "incentive_amount": np.random.normal(loc=5000, scale=1200, size=n).round(2),
    "status":           np.random.choice(["SETTLED", "PENDING", "ERROR", "CANCELLED"], n, p=[0.75, 0.15, 0.07, 0.03]),
}

# Inject intentional data quality issues for demonstration
fin_data["partner_id"][10]       = None    # null partner
fin_data["incentive_amount"][25] = 95000   # outlier — suspiciously high
fin_data["incentive_amount"][26] = -3000   # negative (invalid)
fin_data["transaction_id"][50]   = fin_data["transaction_id"][49]  # duplicate

df_fin = pd.DataFrame(fin_data)
df_fin["transaction_date"] = pd.to_datetime(df_fin["transaction_date"])
df_fin["settlement_date"]  = pd.to_datetime(df_fin["settlement_date"])
df_fin["days_to_settle"]   = (df_fin["settlement_date"] - df_fin["transaction_date"]).dt.days


# ================================================================
# SECTION 2: HEALTHCARE — Customer Case SLA Monitoring
# ================================================================

m = 400
priorities = np.random.choice(["P1", "P2", "P3"], m, p=[0.1, 0.4, 0.5])
sla_map    = {"P1": 4, "P2": 24, "P3": 72}

created_dates    = [datetime.today() - timedelta(days=int(np.random.randint(0, 90))) for _ in range(m)]
resolution_hours = [
    max(1, int(np.random.normal(loc=sla_map[p] * 0.8, scale=sla_map[p] * 0.6)))
    for p in priorities
]

hc_data = {
    "case_id":          [f"CASE{str(i).zfill(5)}" for i in range(1, m + 1)],
    "customer_id":      np.random.choice([f"CUST{str(i).zfill(4)}" for i in range(1, 101)], m),
    "case_type":        np.random.choice(["Equipment Fault", "Software Issue", "Billing", "Service Request"], m),
    "priority":         priorities,
    "assigned_team":    np.random.choice(["Team_Alpha", "Team_Beta", "Team_Gamma"], m),
    "created_date":     created_dates,
    "resolution_hours": resolution_hours,
    "status":           np.random.choice(["Resolved", "Open", "Escalated"], m, p=[0.72, 0.20, 0.08]),
}

# Inject missing data
hc_data["customer_id"][5]   = None
hc_data["assigned_team"][8] = None

df_hc = pd.DataFrame(hc_data)
df_hc["sla_threshold"] = df_hc["priority"].map(sla_map)
df_hc["sla_status"]    = np.where(
    df_hc["resolution_hours"] > df_hc["sla_threshold"], "SLA_BREACH", "WITHIN_SLA"
)


# ================================================================
# MODULE 1: DATA PROFILING (reusable for both domains)
# ================================================================

def profile_dataframe(df, domain_name):
    print("\n" + "=" * 60)
    print(f"DATA QUALITY PROFILE — {domain_name}")
    print(f"Run Date : {datetime.today().strftime('%Y-%m-%d %H:%M')}")
    print(f"Records  : {len(df):,}")
    print("=" * 60)

    null_summary = df.isnull().sum()
    null_pct     = (df.isnull().mean() * 100).round(2)
    null_report  = pd.DataFrame({"null_count": null_summary, "null_pct": null_pct})
    null_report  = null_report[null_report["null_count"] > 0]

    print("\n── NULL / MISSING VALUES ──")
    print("  No null values detected." if null_report.empty else null_report.to_string())

    print("\n── RECORD COUNT BY CATEGORY ──")
    cat_col = "program_code" if "program_code" in df.columns else "case_type"
    print(df[cat_col].value_counts().to_string())


# ================================================================
# MODULE 2: ANOMALY DETECTION — Financial (Z-score on amounts)
# ================================================================

def detect_amount_anomalies(df, column="incentive_amount", threshold=3.0):
    print("\n" + "=" * 60)
    print(f"ANOMALY DETECTION — {column} (|Z-score| > {threshold})")
    print("=" * 60)

    mean = df[column].mean()
    std  = df[column].std()
    df   = df.copy()
    df["z_score"] = ((df[column] - mean) / std).round(2)

    anomalies = df[df["z_score"].abs() > threshold]
    print(f"  Mean: {mean:,.2f}  |  Std Dev: {std:,.2f}")
    print(f"  Anomalies found: {len(anomalies)}\n")
    if not anomalies.empty:
        print(anomalies[["transaction_id", "partner_id", "program_code",
                          column, "z_score", "status"]].to_string(index=False))


# ================================================================
# MODULE 3: SLA BREACH ANALYSIS — Healthcare
# ================================================================

def analyze_sla_breaches(df):
    print("\n" + "=" * 60)
    print("SLA BREACH ANALYSIS — Healthcare Cases")
    print("=" * 60)

    total    = len(df)
    breaches = df[df["sla_status"] == "SLA_BREACH"]
    print(f"  Total Cases  : {total:,}")
    print(f"  SLA Breaches : {len(breaches):,} ({len(breaches)/total*100:.1f}%)")

    print("\n  Breach Rate by Priority:")
    priority_summary = df.groupby("priority").apply(
        lambda x: pd.Series({
            "total":         len(x),
            "breaches":      (x["sla_status"] == "SLA_BREACH").sum(),
            "breach_pct":    round(100.0 * (x["sla_status"] == "SLA_BREACH").sum() / len(x), 1),
            "avg_res_hours": round(x["resolution_hours"].mean(), 1),
        })
    )
    print(priority_summary.to_string())

    print("\n  Breach Rate by Team:")
    team_summary = df.groupby("assigned_team").apply(
        lambda x: pd.Series({
            "total":      len(x),
            "breaches":   (x["sla_status"] == "SLA_BREACH").sum(),
            "breach_pct": round(100.0 * (x["sla_status"] == "SLA_BREACH").sum() / len(x), 1),
        })
    ).sort_values("breach_pct", ascending=False)
    print(team_summary.to_string())


# ================================================================
# MODULE 4: SETTLEMENT GAP ANALYSIS — Financial
# ================================================================

def analyze_settlement_gaps(df, threshold_days=5):
    print("\n" + "=" * 60)
    print(f"SETTLEMENT GAP ANALYSIS (threshold: {threshold_days} days)")
    print("=" * 60)

    breaches = df[df["days_to_settle"] > threshold_days]
    print(f"  Total Records     : {len(df):,}")
    print(f"  Gap Breaches      : {len(breaches):,} ({len(breaches)/len(df)*100:.1f}%)")
    print(f"  Avg Gap (all)     : {df['days_to_settle'].mean():.1f} days")
    if not breaches.empty:
        print(f"  Avg Gap (breaches): {breaches['days_to_settle'].mean():.1f} days")


# ================================================================
# MODULE 5: DUAL-DOMAIN DASHBOARD (4-panel chart)
# ================================================================

def generate_dashboard(df_fin, df_hc):
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.suptitle("Production Support Dashboard — Financial & Healthcare",
                 fontsize=13, fontweight="bold")

    # Panel 1 (Financial): Incentive amount distribution
    axes[0, 0].hist(df_fin["incentive_amount"], bins=40, color="#2E75B6", edgecolor="white")
    axes[0, 0].axvline(df_fin["incentive_amount"].mean(), color="red",
                       linestyle="--", label="Mean")
    axes[0, 0].set_title("Financial — Incentive Amount Distribution")
    axes[0, 0].set_xlabel("Incentive Amount ($)")
    axes[0, 0].set_ylabel("Frequency")
    axes[0, 0].legend()

    # Panel 2 (Financial): Transaction status breakdown
    status_counts = df_fin["status"].value_counts()
    colors_fin    = ["#2E75B6", "#ED7D31", "#FF0000", "#A9A9A9"]
    axes[0, 1].pie(status_counts, labels=status_counts.index,
                   autopct="%1.1f%%", colors=colors_fin[:len(status_counts)], startangle=90)
    axes[0, 1].set_title("Financial — Transaction Status Mix")

    # Panel 3 (Healthcare): SLA breach rate by priority
    sla_summary = df_hc.groupby("priority").apply(
        lambda x: round(100.0 * (x["sla_status"] == "SLA_BREACH").sum() / len(x), 1)
    ).reindex(["P1", "P2", "P3"])
    bars = axes[1, 0].bar(sla_summary.index, sla_summary.values,
                          color=["#FF0000", "#ED7D31", "#FFC000"], edgecolor="white")
    axes[1, 0].set_title("Healthcare — SLA Breach Rate by Priority (%)")
    axes[1, 0].set_xlabel("Priority")
    axes[1, 0].set_ylabel("Breach Rate (%)")
    axes[1, 0].set_ylim(0, 100)
    for bar, val in zip(bars, sla_summary.values):
        axes[1, 0].text(bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + 1, f"{val}%", ha="center", fontsize=10)

    # Panel 4 (Healthcare): Average resolution hours by team
    team_avg = df_hc.groupby("assigned_team")["resolution_hours"].mean().round(1)
    axes[1, 1].bar(team_avg.index, team_avg.values, color="#70AD47", edgecolor="white")
    axes[1, 1].set_title("Healthcare — Avg Resolution Hours by Team")
    axes[1, 1].set_xlabel("Team")
    axes[1, 1].set_ylabel("Hours")
    for i, val in enumerate(team_avg.values):
        axes[1, 1].text(i, val + 0.3, f"{val}h", ha="center", fontsize=10)

    plt.tight_layout()
    plt.savefig("production_support_dashboard.png", dpi=150, bbox_inches="tight")
    print("\n  Chart saved: production_support_dashboard.png")
    plt.show()


# ================================================================
# MAIN — Run all modules
# ================================================================

if __name__ == "__main__":
    # Financial domain
    profile_dataframe(df_fin, "Financial Services — Incentive Transactions")
    detect_amount_anomalies(df_fin, column="incentive_amount", threshold=3.0)
    analyze_settlement_gaps(df_fin, threshold_days=5)

    # Healthcare domain
    profile_dataframe(df_hc, "Healthcare — Customer Cases")
    analyze_sla_breaches(df_hc)

    # Combined visual dashboard
    generate_dashboard(df_fin, df_hc)

    print("\n" + "=" * 60)
    print("REPORT COMPLETE")
    print("=" * 60)
