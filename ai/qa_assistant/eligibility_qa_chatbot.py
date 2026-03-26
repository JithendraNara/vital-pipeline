"""
Eligibility Data QA Assistant — LLM-Powered Natural Language Interface

Ask questions about eligibility data in plain English.
Uses MiniMax (via mcporter) to generate SQL from natural language,
executes it against the dataset, and returns formatted answers.

Usage:
    python eligibility_qa_chatbot.py
    # Then type questions like:
    # > how many members are missing email?
    # > show me the 10 members with the most data quality issues
    # > what states have the most duplicate mem_ids?
"""

import sqlite3
import pandas as pd
import json
import sys
import os
from datetime import datetime

# Check for MiniMax / OpenAI API key
HAS_MINIMAX = os.environ.get("MINIMAX_API_KEY") or os.path.exists(
    os.path.expanduser("~/.openclaw/.credentials.json")
)

try:
    from openai import OpenAI
    HAS_OPENAI = bool(os.environ.get("OPENAI_API_KEY"))
except ImportError:
    HAS_OPENAI = False

# ============================================================
# Schema context for the LLM
# ============================================================
SCHEMA_CONTEXT = """
You are querying a SQLite database called 'eligibility.db' with one table:

Table: eligibility
Columns:
- mem_id (TEXT): Unique member identifier
- first_name (TEXT): Member first name
- last_name (TEXT): Member last name
- dob (TEXT): Date of birth (YYYY-MM-DD format, empty string = missing)
- email (TEXT): Email address (empty string = missing)
- phone (TEXT): Phone number (empty string = missing)
- city (TEXT): City
- state (TEXT): 2-letter US state code
- zip_code (TEXT): Zip code (can be integer-like, leading zeros may be missing)
- effective_date (TEXT): Coverage start date
- termination_date (TEXT): Coverage end date ('Active' = still enrolled)
- covered_relation (TEXT): Self/Spouse/Child/Domestic Partner
- plan_type (TEXT): HMO/PPO/EPO/HDHP/POS
- metal_level (TEXT): Bronze/Silver/Gold/Platinum

Data quality issues to look for:
- zip_code < 10000 = missing leading zeros (should be 5 digits)
- dob = '' = missing date of birth
- email = '' = missing email
- Duplicate mem_id values
- covered_relation = 'Child' but age > 26
- termination_date = 'Active' but there's a termination date in the past
"""

SYSTEM_PROMPT = f"""You are a healthcare data analyst. Given a question about eligibility data,
write a SQL query to answer it.

Rules:
- Output ONLY valid SQLite SQL (no markdown, no explanation)
- The database file is 'eligibility.db'
- Always use proper SQLite syntax
- For text fields that are empty, use condition: column = ''
- For duplicate detection, use: HAVING COUNT(*) > 1
- For missing leading zeros in zip: CAST(zip_code AS INTEGER) < 10000
- LIMIT results to 20 unless asked for more
- Today's date is 2026-03-25
- Age: calculate from dob against today (2026-03-25)
- If the question is not answerable with SQL, output: -- CANNOT ANSWER WITH SQL
{ SCHEMA_CONTEXT }
"""

# ============================================================
# LLM: Generate SQL from natural language
# ============================================================

def generate_sql(question: str, model: str = "minimax/MiniMax-M2.7") -> str:
    """Use LLM to convert natural language question to SQL."""
    try:
        client = OpenAI(
            api_key=os.environ.get("MINIMAX_API_KEY"),
            base_url="https://api.minimax.io/v1",
        )
    except Exception:
        try:
            client = OpenAI(
                api_key=os.environ.get("OPENAI_API_KEY"),
            )
        except Exception:
            return "-- LLM not configured (set MINIMAX_API_KEY or OPENAI_API_KEY)"

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
        temperature=0,
        max_tokens=300,
    )

    sql = response.choices[0].message.content.strip()
    # Remove markdown code blocks if present
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
    return sql


def run_sql(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    """Execute SQL and return results as DataFrame."""
    try:
        # Only allow SELECT statements for safety
        if not sql.strip().upper().startswith("SELECT"):
            return pd.DataFrame({"error": ["Query must be a SELECT statement"]})

        # Safety: reject DROP, DELETE, UPDATE, INSERT, ALTER, etc.
        dangerous = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "CREATE"]
        for kw in dangerous:
            if kw in sql.upper():
                return pd.DataFrame({"error": [f"Operation '{kw}' is not allowed"]})

        df = pd.read_sql_query(sql, conn)
        return df
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


def format_results(question: str, sql: str, df: pd.DataFrame) -> str:
    """Format query results as a readable response."""
    if "error" in df.columns:
        return f"⚠️ Error: {df['error'].iloc[0]}"

    if len(df) == 0:
        return "✅ No results found."

    rows = len(df)
    cols = list(df.columns)

    # Truncate for display
    display_df = df.copy()
    for col in display_df.columns:
        if display_df[col].dtype == "float64":
            display_df[col] = display_df[col].round(2)

    # Build response
    lines = []
    lines.append(f"**Q:** {question}")
    lines.append(f"**SQL:** `{sql.strip()}`")
    lines.append(f"**Results:** {rows} row{'s' if rows > 1 else ''}")
    lines.append("")
    lines.append(display_df.to_string(index=False))
    lines.append("")
    lines.append(f"_{datetime.now().strftime('%H:%M:%S')}_")

    return "\n".join(lines)


# ============================================================
# Demo: Run pre-set questions
# ============================================================

DEMO_QUESTIONS = [
    "How many members are missing their email address?",
    "Which state has the most members?",
    "Show me the 10 members with the highest data quality issues (missing fields)",
    "How many members are children over age 26?",
    "List all the zip codes that have lost their leading zeros",
    "How many members have duplicate mem_ids?",
    "What's the breakdown of members by plan type?",
    "Show me all members in New York with missing zip codes",
]


def run_demo(db_path: str = "data/eligibility.db"):
    """Run a set of pre-defined questions against the database."""
    conn = sqlite3.connect(db_path)

    print("=" * 60)
    print("ELIGIBILITY QA ASSISTANT — DEMO MODE")
    print("=" * 60)
    print()

    for i, question in enumerate(DEMO_QUESTIONS, 1):
        print(f"[{i}/{len(DEMO_QUESTIONS)}] {question}")
        print()

        sql = generate_sql(question)
        print(f"SQL: {sql.strip()}")

        df = run_sql(conn, sql)
        if "error" not in df.columns:
            print(f"→ {len(df)} results")
            if len(df) > 0:
                print(df.head(5).to_string(index=False))
        else:
            print(f"⚠️ {df['error'].iloc[0]}")

        print()
        print("-" * 40)
        print()

    conn.close()


# ============================================================
# Interactive mode
# ============================================================

def interactive(db_path: str = "data/eligibility.db"):
    """Run interactive QA session."""
    conn = sqlite3.connect(db_path)

    print("=" * 60)
    print("ELIGIBILITY QA ASSISTANT — INTERACTIVE MODE")
    print("=" * 60)
    print("Ask questions about the eligibility data in plain English.")
    print("Type 'exit' or 'quit' to stop.")
    print("Type 'demo' to run pre-set questions.")
    print()

    while True:
        try:
            question = input("❓ Your question: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question:
            continue

        if question.lower() in ["exit", "quit", "q"]:
            print("Goodbye!")
            break

        if question.lower() == "demo":
            conn.close()
            run_demo(db_path)
            conn = sqlite3.connect(db_path)
            continue

        print()

        # Generate SQL using LLM
        print("🤖 Generating SQL...")
        sql = generate_sql(question)
        print(f"SQL: {sql.strip()}")
        print()

        # Execute
        print("📊 Running query...")
        df = run_sql(conn, sql)
        result = format_results(question, sql, df)
        print(result)
        print()


if __name__ == "__main__":
    # Check if demo mode
    import argparse
    parser = argparse.ArgumentParser(description="Eligibility QA Assistant")
    parser.add_argument("--db", default="data/eligibility.db", help="Path to SQLite DB")
    parser.add_argument("--demo", action="store_true", help="Run demo questions")
    args = parser.parse_args()

    if args.demo:
        run_demo(args.db)
    else:
        interactive(args.db)
