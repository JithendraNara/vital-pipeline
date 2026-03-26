"""
Great Expectations data quality suite for eligibility data.
Run: python run_gx_suite.py
"""
import great_expectations as gx
import pandas as pd
from datetime import datetime

# Initialize GX context
context = gx.get_context()

# ============================================================
# Expectations — Member Demographics
# ============================================================
demographics_suite = gx.dataset.Suite(
    name="eligibility_demographics_quality",
    expectations=[
        # ID integrity
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="mem_id"
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="mem_id"
        ),
        # Demographics
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="first_name"
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="last_name"
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="date_of_birth"
        ),
        # Age reasonability
        gx.expectations.ExpectColumnValueLengthsToBeBetween(
            column="date_of_birth",
            min_value=8,  # YYYY-MM-DD
            max_value=10,
        ),
        # Email format
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="email",
            regex="^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
            mostly=0.95,
        ),
        # Phone format
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="phone",
            regex=r"^\d{3}-\d{3}-\d{4}$",
            mostly=0.90,
        ),
        # Zip code — 5 digits
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="zip_code",
            regex=r"^\d{5}$",
            mostly=0.99,
        ),
        # State — valid US states
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="state",
            value_set=["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
                       "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
                       "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
                       "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
                       "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY"],
        ),
    ]
)


# ============================================================
# Expectations — Coverage Rules
# ============================================================
coverage_suite = gx.dataset.Suite(
    name="eligibility_coverage_rules",
    expectations=[
        # Dates
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="effective_date"
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="effective_date",
            type_="datetime64"
        ),
        # Coverage dates make sense
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="effective_date",
            min_value=datetime(2000, 1, 1),
            max_value=datetime(2030, 12, 31),
        ),
        # Plan types
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="plan_type",
            value_set=["HMO", "PPO", "EPO", "HDHP", "POS"],
            mostly=0.99,
        ),
        # Metal levels
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="metal_level",
            value_set=["Bronze", "Silver", "Gold", "Platinum"],
            mostly=0.98,
        ),
        # Relationship
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="covered_relation",
            value_set=["Self", "Spouse", "Child", "Domestic Partner"],
        ),
        # Active-only: if termination_date is null or 'Active', member is active
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="termination_date",
            mostly=0.95,
        ),
    ]
)


# ============================================================
# Run Suite Against Data
# ============================================================
def run_eligibility_dq(data_path: str, output_path: str = None):
    """
    Run all eligibility DQ suites against a CSV file.

    Args:
        data_path: Path to eligibility CSV
        output_path: Optional path to save HTML report
    """
    print(f"Running Eligibility DQ Suite...")
    print(f"Source: {data_path}")

    # Load data
    df = pd.read_csv(data_path)

    # Rename columns to match GX expectations
    df = df.rename(columns={
        "zip_code": "zip_code",  # Already correct
        "effective_date": "effective_date",
        "termination_date": "termination_date",
    })

    print(f"Records: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    # Validate demographics
    print("\n[1/2] Running demographics suite...")
    demographics_results = demographics_suite.validate(df)
    print(f"  Passed: {demographics_results.success}")
    print(f"  Results: {len(demographics_results.results)} expectations")

    # Validate coverage
    print("\n[2/2] Running coverage rules suite...")
    coverage_results = coverage_suite.validate(df)
    print(f"  Passed: {coverage_results.success}")
    print(f"  Results: {len(coverage_results.results)} expectations")

    # Aggregate results
    all_results = {
        "run_time": datetime.now().isoformat(),
        "source_file": data_path,
        "record_count": len(df),
        "demographics_passed": demographics_results.success,
        "coverage_passed": coverage_results.success,
        "overall_passed": demographics_results.success and coverage_results.success,
    }

    if output_path:
        # Build HTML report
        gx.renderers.DefaultJinjaPageViewRenderer.to_html_file(
            demographics_results,
            output_path.replace(".json", "_demographics.html")
        )
        gx.renderers.DefaultJinjaPageViewRenderer.to_html_file(
            coverage_results,
            output_path.replace(".json", "_coverage.html")
        )
        print(f"\nReports saved to: {output_path}")

    # Print failed expectations
    for suite_name, results in [
        ("Demographics", demographics_results),
        ("Coverage", coverage_results),
    ]:
        failed = [r for r in results.results if not r.success]
        if failed:
            print(f"\n⚠️  {suite_name} — {len(failed)} failures:")
            for f in failed[:5]:
                print(f"  • {f.expectation_config['kwargs']}: {f.exception_info}")

    return all_results


if __name__ == "__main__":
    import sys
    data_path = sys.argv[1] if len(sys.argv) > 1 else "data/eligibility_dirty.csv"
    result = run_eligibility_dq(data_path, "data/dq_report.html")
    print(f"\n{'✅ ALL PASSED' if result['overall_passed'] else '❌ FAILURES FOUND'}")
