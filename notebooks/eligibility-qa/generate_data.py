"""
Generate synthetic eligibility file for QA demonstration.
Creates a realistic but fake dataset with deliberate errors for QA practice.
"""
import pandas as pd
import random
from datetime import datetime, timedelta
import hashlib

# Seed for reproducibility
random.seed(42)

N_MEMBERS = 228  # Match Sword Health's case study size

# Realistic first names and last names
FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
               "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
               "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Christopher",
               "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Margaret",
               "Mark", "Sandra", "Donald", "Ashley", "Steven", "Kimberly", "Paul",
               "Emily", "Andrew", "Donna", "Joshua", "Michelle", "Kenneth", "Dorothy",
               "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa", "Timothy"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
              "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
              "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
              "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
              "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
              "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green"]

CITIES = ["Fort Wayne", "Indianapolis", "Chicago", "Houston", "Phoenix", "Dallas",
           "San Antonio", "Los Angeles", "New York", "Miami"]

STATES = ["IN", "IL", "TX", "AZ", "FL", "CA", "NY"]

# Real ICD-10 codes (sample)
ICD10_CODES = [
    "E11.9", "I10", "J44.1", "K21.0", "N18.3", "R73.03",
    "E78.5", "I25.10", "J44.0", "M79.3", "F32.9", "R51"
]

# Generate member IDs
def gen_mem_id(idx):
    return f"MEM{str(idx).zfill(6)}"

# Generate dates
def random_dob():
    start = datetime(1940, 1, 1)
    end = datetime(2010, 12, 31)
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

def random_effective():
    base = datetime(2025, 1, 1)
    return base + timedelta(days=random.randint(0, 365))

def random_termination(active_only=False):
    if active_only or random.random() > 0.15:
        return None  # Active
    return datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))

# Generate base records
records = []
for i in range(N_MEMBERS):
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email_provider = random.choice(["gmail.com", "yahoo.com", "outlook.com"])
    
    dob = random_dob()
    effective = random_effective()
    terminated = random_termination()
    
    # Northeast zip codes that need leading zero preservation
    state = random.choice(STATES)
    if state in ["NY", "MA", "CT", "ME", "VT", "NH", "RI"]:
        zip_code = str(random.randint(1, 9999)).zfill(5)  # Some will lose leading zero
    else:
        zip_code = str(random.randint(10000, 99999))
    
    records.append({
        "mem_id": gen_mem_id(i + 1),
        "first_name": first,
        "last_name": last,
        "dob": dob.strftime("%Y-%m-%d"),
        "email": f"{first.lower()}.{last.lower()}@{email_provider}",
        "phone": f"260-{random.randint(200,999)}-{random.randint(1000,9999)}",
        "address": f"{random.randint(100,9999)} {random.choice(['Main', 'Oak', 'Pine', 'Maple', 'Cedar'])} St",
        "city": random.choice(CITIES),
        "state": state,
        "zip_code": zip_code,
        "effective_date": effective.strftime("%Y-%m-%d"),
        "termination_date": terminated.strftime("%Y-%m-%d") if terminated else "Active",
        "covered_relation": random.choice(["Self", "Self", "Self", "Spouse", "Child", "Domestic Partner"]),
        "primary_care_id": f"DR{str(random.randint(1000, 9999))}",
        "plan_type": random.choice(["HMO", "PPO", "EPO", "HDHP"]),
        "metal_level": random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
        "hsa_eligible": random.choice(["Yes", "No"]),
        "daw_code": random.choice(["", "", "", "Y"]),  # Dispense as written
    })

df = pd.DataFrame(records)

# ============================================================
# INTRODUCE DELIBERATE ERRORS (matching Sword Health case study)
# ============================================================

# ERROR 1: Zip codes stored as integers — Northeast zips lose leading zeros
# (50 Northeast members: zip codes 001xx → stored as 1xx)
northeast_mask = df["state"].isin(["NY", "MA", "CT", "ME", "VT", "NH", "RI"])
northeast_idx = df[northeast_mask].index[:50]
for idx in northeast_idx:
    original = df.loc[idx, "zip_code"]
    stripped = str(int(original))  # Simulates integer storage
    df.loc[idx, "zip_code"] = stripped

# ERROR 2: Duplicate mem_ids (5 pairs = 10 records)
duplicate_ids = [
    ("MEM00082", "MEM00195"),  # Same person, two plan enrollments
    ("MEM00019", "MEM00141"),  # Twin enrolled twice
    ("MEM00034", "MEM00203"),  # Data entry error
    ("MEM00057", "MEM00178"),  # System merge error
    ("MEM00088", "MEM00162"),  # Fraud/multiple fake enrollments
]
for dup_pair in duplicate_ids:
    for dup_id in dup_pair:
        idx = df[df["mem_id"] == dup_id].index[0]
        df.loc[idx, "mem_id"] = dup_pair[0]  # Both get same ID

# ERROR 3: Children over 26 — covered_relation field errors
overage_indices = df.sample(47, random_state=42).index
for idx in overage_indices:
    age = (datetime(2025, 1, 1) - pd.to_datetime(df.loc[idx, "dob"])).days / 365
    if age < 27:  # Make them actually under 27 first
        df.loc[idx, "dob"] = (datetime(1990, 1, 1) + timedelta(days=random.randint(0, 5000))).strftime("%Y-%m-%d")
    df.loc[idx, "covered_relation"] = "Child"  # But marked as child

# ERROR 4: Missing data (6 no DOB, 10 no email, 7 no phone)
missing_dob = df.sample(6, random_state=123).index
df.loc[missing_dob, "dob"] = ""

missing_email = df.sample(10, random_state=456).index
df.loc[missing_email, "email"] = ""

missing_phone = df.sample(7, random_state=789).index
df.loc[missing_phone, "phone"] = ""

# ERROR 5: 3 inactive records marked as active (termination_date but still enrolled)
inactive_indices = df[(df["termination_date"] != "Active")].sample(3, random_state=999).index
df.loc[inactive_indices, "termination_date"] = "Active"

# ============================================================
# Save to CSV (integer zip codes — mimics the original error)
# ============================================================
df["zip_code"] = df["zip_code"].astype(str)  # But store as string in this version

# Save with different formats for different stages
df.to_csv("data/eligibility_dirty.csv", index=False)
print(f"Generated {len(df)} records → data/eligibility_dirty.csv")
print(f"\nError summary:")
print(f"  - Zip codes stored as integers: ~50 (00123 → 123)")
print(f"  - Duplicate mem_ids: 10 records (5 pairs)")
print(f"  - Children marked as dependents past age 26: 47")
print(f"  - Missing DOB: 6 | Missing email: 10 | Missing phone: 7")
print(f"  - Terminated but marked active: 3")
