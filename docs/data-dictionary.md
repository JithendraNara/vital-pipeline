# Data Dictionary — Vital Pipeline

## Member Eligibility (`eligibility` table)

| Column | Type | Description | Example | DQ Rules |
|--------|------|-------------|---------|---------|
| `mem_id` | VARCHAR(50) | Unique member identifier | `MEM000123` | NOT NULL, UNIQUE |
| `first_name` | VARCHAR(100) | Member first name | `James` | NOT NULL |
| `last_name` | VARCHAR(100) | Member last name | `Smith` | NOT NULL |
| `date_of_birth` | DATE | Date of birth | `1985-03-15` | NOT NULL, reasonable age (< 120 years) |
| `email` | VARCHAR(255) | Email address | `james.smith@gmail.com` | Valid email format, mostly NOT NULL |
| `phone` | VARCHAR(20) | Phone number | `260-555-1234` | Format: `XXX-XXX-XXXX` |
| `address` | VARCHAR(255) | Street address | `123 Main St` | NOT NULL |
| `city` | VARCHAR(100) | City | `Fort Wayne` | NOT NULL |
| `state` | VARCHAR(2) | 2-letter US state code | `IN` | 50 valid US state codes |
| `zip_code` | VARCHAR(5) | 5-digit US zip code | `46802` | NOT NULL, 5 digits, zero-padded |
| `coverage_effective_date` | DATE | When coverage began | `2025-01-01` | NOT NULL |
| `coverage_termination_date` | DATE | When coverage ended (NULL = active) | `2025-12-31` | NULL means member is active |
| `covered_relation` | VARCHAR(50) | Relationship to primary | `Self`, `Spouse`, `Child`, `Domestic Partner` | NOT NULL |
| `plan_type` | VARCHAR(10) | Health plan type | `PPO`, `HMO`, `EPO`, `HDHP` | NOT NULL |
| `metal_level` | VARCHAR(20) | ACA metal tier | `Bronze`, `Silver`, `Gold`, `Platinum` | NULL allowed |
| `hsa_eligible` | BOOLEAN | HSA-qualified plan | `TRUE`, `FALSE` | NOT NULL |

## Key Business Rules

### Eligibility Active Status
A member is considered **active** if:
- `coverage_termination_date IS NULL`, OR
- `coverage_termination_date >= CURRENT_DATE`

### Dependent Age Limit
- Dependents with `covered_relation = 'Child'` are eligible through the end of the month in which they turn **26**
- Age is calculated from `date_of_birth` against the analysis date

### Zip Code Standardization
- All zip codes must be **zero-padded to 5 digits**
- Northeast zip codes (MA, CT, NY, etc.) starting with `00xxx` are stored as 4-digit integers in source systems
- **Fix:** Cast to string and zero-pad: `LPAD(zip_code::VARCHAR, 5, '0')`

## Healthcare Domain Terms

| Term | Definition |
|------|------------|
| **PMPM** | Per Member Per Month — cost metric dividing total cost by member months |
| **VBC** | Value-Based Care — payment model tied to quality and outcomes |
| **HCC** | Hierarchical Condition Category — risk adjustment model used by CMS |
| **ICD-10** | International Classification of Diseases, 10th Revision |
| **CPT** | Current Procedural Terminology — physician billing codes |
| **DRG** | Diagnosis-Related Group — hospital billing classification |
| **HSA** | Health Savings Account — tax-advantaged account for HDHP plans |
| **HDHP** | High-Deductible Health Plan — plan with minimum $1,400 deductible |
| **MLR** | Medical Loss Ratio — % of premium spent on clinical services |
| **ACO** | Accountable Care Organization — VBC provider network |

## Data Model (dbt)

```
raw.eligibility          → stg_eligibility_members
                                  ↓
                          int_member_months (member-month grain)
                                  ↓
                          mart_member_roster (analytics-ready)
```

## ICD-10 Reference Codes Used

| Code | Description | HCC Weight |
|------|-------------|-----------|
| E11.9 | Type 2 diabetes | 1.0 |
| I10 | Hypertension | 0.97 |
| J44.1 | COPD | 0.72 |
| K21.0 | GERD | 0.31 |
| N18.3 | Chronic kidney disease | 0.78 |
| R73.03 | Prediabetes | 0.27 |
| E78.5 | Hyperlipidemia | 0.54 |
| I25.10 | Atherosclerotic heart disease | 0.80 |

## Notes

- This data dictionary is generated from dbt column descriptions
- Update dbt schema.yml to maintain this dictionary
- Generate with: `dbt docs generate`
