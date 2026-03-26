"""
Generate synthetic claims data for anomaly detection demonstration.
Creates realistic medical and pharmacy claims with embedded anomalies.
"""
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

N_MEMBERS = 200
N_CLAIMS = 3000

# Generate members
member_ids = [f"MEM{str(i).zfill(6)}" for i in range(1, N_MEMBERS + 1)]

# Provider specialties and their typical cost ranges
SPECIALTIES = {
    "Primary Care": (80, 250),
    "Cardiology": (200, 800),
    "Orthopedics": (300, 1500),
    "Dermatology": (100, 400),
    "Psychiatry": (150, 500),
    "Emergency Medicine": (300, 2000),
    "Neurology": (250, 900),
    "Oncology": (500, 5000),
    "Radiology": (150, 800),
}

# Service types with CPT code ranges
SERVICE_TYPES = [
    ("Office Visit", 50, 300, "CPT-99"),
    ("Lab Work", 20, 200, "CPT-80"),
    ("Imaging", 100, 800, "CPT-70"),
    ("Procedure", 200, 2000, "CPT-10"),
    ("Emergency", 300, 3000, "CPT-99"),
    ("Specialist", 150, 600, "CPT-90"),
    ("Physical Therapy", 80, 400, "CPT-97"),
]

# ICD-10 diagnoses
DIAGNOSES = [
    ("E11.9", "Type 2 Diabetes", 0.08),
    ("I10", "Hypertension", 0.12),
    ("J44.1", "COPD", 0.05),
    ("M54.5", "Low Back Pain", 0.10),
    ("F32.9", "Depression", 0.08),
    ("R51", "Headache", 0.07),
    ("K21.0", "GERD", 0.06),
    ("E78.5", "Hyperlipidemia", 0.09),
    ("I25.10", "Heart Disease", 0.06),
    ("Z23", "Preventive Care", 0.12),
    ("J45.909", "Asthma", 0.05),
    ("N18.3", "CKD", 0.03),
    ("R73.03", "Prediabetes", 0.04),
    ("M79.3", "Panniculitis", 0.03),
    ("Z00.00", "General Exam", 0.08),
]

def weighted_choice(choices):
    codes, descs, weights = zip(*choices)
    return random.choices(codes, weights=weights, k=1)[0]

def random_date(start_days_ago=365):
    return datetime.now() - timedelta(
        days=random.randint(0, start_days_ago),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )

claims = []
for i in range(N_CLAIMS):
    mem_id = random.choice(member_ids)
    specialty, cost_range, cpt = random.choice(list(SPECIALTIES.items()))
    service_name, min_cost, max_cost, cpt_range = random.choice(SERVICE_TYPES)

    # Base claim
    allowed_amount = random.uniform(min_cost, max_cost)
    # Copay (member responsibility)
    copay = min(random.uniform(20, 60), allowed_amount * 0.3)
    # Insurance pays the rest
    paid_amount = allowed_amount - copay

    # Some random date in the last year
    claim_date = random_date()

    claims.append({
        "claim_id": f"CLM{str(i+1).zfill(7)}",
        "mem_id": mem_id,
        "claim_date": claim_date.strftime("%Y-%m-%d"),
        "claim_type": random.choice(["medical", "medical", "pharmacy"]),
        "service_type": service_name,
        "specialty": specialty,
        "cpt_code": f"{cpt_range[:3]}{random.randint(1000, 9999)}",
        "icd10_code": weighted_choice(DIAGNOSES),
        "allowed_amount": round(allowed_amount, 2),
        "paid_amount": round(paid_amount, 2),
        "member_copay": round(copay, 2),
        "provider_npi": f"NPI{random.randint(1000000000, 9999999999)}",
        "place_of_service": random.choice(["Office", "Hospital", "Urgent Care", "Telehealth"]),
    })

df = pd.DataFrame(claims)

# ============================================================
# EMBED ANOMALIES (not visible in the data initially)
# ============================================================

# ANOMALY 1: 15 members with suspiciously high costs (3+ std dev above mean)
mean_cost = df["paid_amount"].mean()
std_cost = df["paid_amount"].std()
high_cost_threshold = mean_cost + 3 * std_cost

# Pick 15 random members and inflate their claims
high_cost_members = random.sample(member_ids, 15)
for mem_id in high_cost_members:
    mask = df["mem_id"] == mem_id
    n_claims = mask.sum()
    # Replace some claims with very high amounts
    for idx in df[mask].sample(min(3, n_claims), random_state=42).index:
        df.loc[idx, "paid_amount"] = round(random.uniform(high_cost_threshold * 1.5, high_cost_threshold * 3), 2)
        df.loc[idx, "allowed_amount"] = df.loc[idx, "paid_amount"] + random.uniform(10, 50)

# ANOMALY 2: 8 members with very frequent visits (potential fraud)
frequent_visitors = random.sample(member_ids, 8)
for mem_id in frequent_visitors:
    mask = df["mem_id"] == mem_id
    existing_claims = df[mask]
    # Add 20 extra claims to each of these members
    for j in range(20):
        claim_date = random_date()
        specialty, cost_range, cpt = random.choice(list(SPECIALTIES.items()))
        service_name, min_cost, max_cost, cpt_range = random.choice(SERVICE_TYPES)
        allowed = random.uniform(min_cost, max_cost)
        claims.append({
            "claim_id": f"CLM{str(N_CLAIMS + len(high_cost_members)*3 + j + 1).zfill(7)}",
            "mem_id": mem_id,
            "claim_date": claim_date.strftime("%Y-%m-%d"),
            "claim_type": "medical",
            "service_type": service_name,
            "specialty": specialty,
            "cpt_code": f"{cpt_range[:3]}{random.randint(1000, 9999)}",
            "icd10_code": weighted_choice(DIAGNOSES),
            "allowed_amount": round(allowed, 2),
            "paid_amount": round(allowed * 0.85, 2),
            "member_copay": round(allowed * 0.15, 2),
            "provider_npi": f"NPI{random.randint(1000000000, 9999999999)}",
            "place_of_service": random.choice(["Office", "Hospital"]),
        })

df = pd.DataFrame(claims)

# ANOMALY 3: 5 claims with exact same amount (potential billing fraud)
exact_amount = round(random.uniform(499, 501), 2)  # Just under $500
for j in range(5):
    idx = random.randint(0, len(df) - 1)
    df.loc[idx, "allowed_amount"] = exact_amount
    df.loc[idx, "paid_amount"] = exact_amount * 0.9
    df.loc[idx, "member_copay"] = exact_amount * 0.1

df = df.reset_index(drop=True)

# Save
df.to_csv("data/claims_dirty.csv", index=False)

# Summary
print(f"Generated {len(df)} claims for {N_MEMBERS} members")
print(f"  Mean paid amount: ${df['paid_amount'].mean():.2f}")
print(f"  Std paid amount: ${df['paid_amount'].std():.2f}")
print(f"  High-cost threshold (3σ): ${high_cost_threshold:.2f}")
print(f"  Anomalous members (high cost): {len(high_cost_members)}")
print(f"  Anomalous members (frequent visits): {len(frequent_visitors)}")
print(f"  Exact-amount suspicious claims: 5")
print(f"\nSaved: data/claims_dirty.csv")
