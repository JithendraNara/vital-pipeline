"""
prefect_flows/eligibility_prefect_flow.py
Prefect 3.x — Healthcare Eligibility Pipeline

Demonstrates modern analytics engineering orchestration:
- Prefect instead of Airflow (greenfield choice at health tech companies)
- dbt Core integration via Prefect dbt plugin
- Great Expectations data quality checks as Prefect task
- S3 → PostgreSQL ingestion with retry logic
- Slack alerting on failure
- Deployment via YAML (infrastructure-as-code)

Run:
    prefect deploy eligibility_pipeline/deployments
    prefect worker start --pool vital-pipeline-pool

Usage:
    prefect deployment run "Eligibility Pipeline/prod-deployment"
    # or trigger via API:
    curl -X POST https://api.prefect.cloud/api/prefect/deployments/<id>/run
"""

from datetime import datetime, timedelta
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.blocks.system import SlackWebhook
from prefect.filesystems import RemoteFileSystem
from prefect.artifacts import create_markdown_artifact

# dbt integration
from prefect_dbt import DbtCoreOperation, results_to_artifacts
from dbt.cli.commands import DbtFlags

# Data quality
from great_expectations.chaconfig import GXConfig
from great_expectations.datasource.fluent import PandasFilesystemDatasource
from great_expectations.checkpoint import SimpleCheckpoint

import pandas as pd
import boto3
from botocore.exceptions import ClientError


# ============================================================
# Task: Validate source file exists and is not empty
# ============================================================

@task(
    name="Validate Source File",
    description="Check eligibility CSV exists in S3 and has content",
    retries=2,
    retry_delay_seconds=30,
    tags=["validation", "eligibility"],
)
def validate_source_file(bucket: str, key: str) -> dict:
    logger = get_run_logger()

    s3 = boto3.client("s3")
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        size_bytes = response["ContentLength"]
        last_modified = response["LastModified"].isoformat()

        logger.info(f"Source file found: s3://{bucket}/{key}")
        logger.info(f"  Size: {size_bytes:,} bytes")
        logger.info(f"  Last modified: {last_modified}")

        if size_bytes == 0:
            raise ValueError("Source file is empty")

        return {
            "bucket": bucket,
            "key": key,
            "size_bytes": size_bytes,
            "last_modified": last_modified,
        }

    except ClientError as e:
        logger.error(f"S3 error: {e}")
        raise


# ============================================================
# Task: Load CSV from S3 into PostgreSQL
# ============================================================

@task(
    name="Load CSV to PostgreSQL",
    description="Download eligibility CSV from S3 and load into staging table",
    retries=3,
    retry_delay_seconds=60,
    tags=["ingestion", "postgres"],
)
def load_csv_to_postgres(source_info: dict, schema: str, table: str) -> dict:
    import io

    logger = get_run_logger()
    s3 = boto3.client("s3")

    # Download CSV
    obj = s3.get_object(Bucket=source_info["bucket"], Key=source_info["key"])
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    logger.info(f"Loaded {len(df):,} rows from S3")

    # Connect to PostgreSQL (credentials from env or Prefect secret)
    import os
    from sqlalchemy import create_engine

    engine = create_engine(os.environ["DATABASE_URL"])

    # Load to staging table (replace each run)
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    engine.dispose()
    logger.info(f"Loaded {len(df):,} rows into {schema}.{table}")

    return {
        "rows_loaded": len(df),
        "schema": schema,
        "table": table,
        "columns": list(df.columns),
    }


# ============================================================
# Task: dbt Staging — Run staging models
# ============================================================

@task(
    name="dbt Staging",
    description="Run dbt staging models (stg_eligibility_members)",
    tags=["dbt", "staging"],
)
def run_dbt_staging() -> dict:
    logger = get_run_logger()

    dbt_op = DbtCoreOperation(
        project_path="/opt/prefect/dbt/synthea_omop_pipeline",
        # Use flags object for Prefect 3.x / dbt 1.8+
        flags=DbtFlags(),
        profiles_path="/opt/prefect/dbt/.dbt/profiles.yml",
        target="prod",
    )

    # Run only staging models
    dbt_op.run(args=["dbt", "run", "--select", "staging", "--target", "prod"])

    logger.info("dbt staging models complete")
    return {"step": "dbt_staging", "status": "success"}


# ============================================================
# Task: dbt Intermediate + Marts
# ============================================================

@task(
    name="dbt Intermediate + Marts",
    description="Run dbt intermediate and marts models",
    tags=["dbt", "marts"],
    trigger=lambda results: all(r.successful for r in results),  # Depends on staging
)
def run_dbt_marts() -> dict:
    logger = get_run_logger()

    dbt_op = DbtCoreOperation(
        project_path="/opt/prefect/dbt/synthea_omper_pipeline",
        flags=DbtFlags(),
        profiles_path="/opt/prefect/dbt/.dbt/profiles.yml",
        target="prod",
    )

    dbt_op.run(args=["dbt", "run", "--select", "intermediate,marts", "--target", "prod"])

    logger.info("dbt intermediate + marts complete")
    return {"step": "dbt_marts", "status": "success"}


# ============================================================
# Task: dbt Tests
# ============================================================

@task(
    name="dbt Tests",
    description="Run dbt singular and generic tests",
    tags=["dbt", "testing"],
    retries=1,
)
def run_dbt_tests() -> dict:
    logger = get_run_logger()

    dbt_op = DbtCoreOperation(
        project_path="/opt/prefect/dbt/synthea_omop_pipeline",
        flags=DbtFlags(),
        profiles_path="/opt/prefect/dbt/.dbt/profiles.yml",
        target="prod",
    )

    result = dbt_op.run(args=["dbt", "test", "--target", "prod"])
    results_artifacts = results_to_artifacts(result)

    logger.info(f"dbt tests complete: {len(results_artifacts)} test results")
    return {"step": "dbt_tests", "status": "success", "n_tests": len(results_artifacts)}


# ============================================================
# Task: Great Expectations Check
# ============================================================

@task(
    name="Great Expectations Suite",
    description="Run GX data quality checks on eligibility data",
    tags=["data-quality", "great-expectations"],
    retries=1,
)
def run_gx_check(context_path: str, batch_path: str) -> dict:
    logger = get_run_logger()

    import great_expectations as gx

    context = gx.get_context(mode="file", project_root_dir=context_path)
    datasource = context.sources.add_pandas_filesystem(
        name="eligibility_filesystem",
        base_directory=batch_path,
    )
    asset = datasource.add_csv_asset(
        name="eligibility_asset",
        batching_regex=".*eligibility.*\\.csv$",
    )
    batch_request = asset.build_batch_request()

    # Run the default expectation suite
    checkpoint = SimpleCheckpoint(
        name="eligibility_checkpoint",
        data_context=context,
        batch_request=batch_request,
    )
    result = checkpoint.run()

    logger.info(f"GX result: {result.success}")
    logger.info(f"  Expectation suite: {result.suite_name}")
    logger.info(f"  Results: {result.statistics}")

    return {
        "step": "gx_check",
        "success": result.success,
        "statistics": result.statistics,
    }


# ============================================================
# Task: Generate QA Summary
# ============================================================

@task(
    name="Generate QA Summary",
    description="Query dbt run results and produce QA summary artifact",
    tags=["reporting"],
)
def generate_qa_summary(
    dbt_result: dict,
    gx_result: dict,
    load_result: dict,
) -> dict:
    logger = get_run_logger()

    summary = f"""
## Eligibility Pipeline QA Summary

**Run:** {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}

### Data Load
- Rows loaded: **{load_result['rows_loaded']:,}**
- Columns: {len(load_result['columns'])}
- Table: `{load_result['schema']}.{load_result['table']}`

### dbt Transformation
- Step: `{dbt_result['step']}`
- Status: **{dbt_result['status']}**
- Tests run: {dbt_result.get('n_tests', 'N/A')}

### Data Quality (Great Expectations)
- Overall success: **{'✅ PASS' if gx_result['success'] else '❌ FAIL'}**
- Total expectations: {gx_result['statistics'].get('total', 'N/A')}
- Successful: {gx_result['statistics'].get('successful', 'N/A')}
- Failed: {gx_result['statistics'].get('failed', 'N/A')}

### Pipeline Status
{'✅ ALL CHECKS PASSED' if (dbt_result['status'] == 'success' and gx_result['success']) else '❌ CHECKS FAILED — REVIEW REQUIRED'}
"""

    logger.info(summary)

    # Create Prefect artifact (visible in Prefect UI)
    create_markdown_artifact(
        markdown=summary,
        key="eligibility-qa-summary",
        description="Eligibility pipeline QA summary",
    )

    return {"summary": summary, "status": "complete"}


# ============================================================
# Task: Notify on failure
# ============================================================

@task(
    name="Notify on Failure",
    description="Send Slack notification when pipeline fails",
    tags=["alerting", "slack"],
    retry_policy=None,
)
def notify_failure(context: dict) -> None:
    logger = get_run_logger()

    try:
        slack_block = SlackWebhook.load("vital-pipeline-alerts")
        slack_block.notify(
            f"❌ Eligibility Pipeline FAILED\n"
            f"Run: {context['run_id']}\n"
            f"Task: {context.get('failed_task', 'unknown')}\n"
            f"Time: {datetime.now().strftime('%H:%M UTC')}\n"
            f"<https://app.prefect.cloud/runs/{context['run_id']}|View in Prefect>"
        )
    except Exception as e:
        logger.warning(f"Slack notification failed: {e}")


# ============================================================
# Main Pipeline Flow
# ============================================================

@flow(
    name="Eligibility Pipeline",
    description="End-to-end eligibility data pipeline: S3 → PostgreSQL → dbt → GX → Slack",
    log_prints=True,
    retries=1,
    retry_delay_seconds=120,
)
def eligibility_pipeline(
    s3_bucket: str = "vital-pipeline-bronze",
    s3_key: str = "eligibility/eligibility_dirty.csv",
    target_schema: str = "staging",
    target_table: str = "eligibility_members",
):
    """
    Main Prefect pipeline for eligibility data processing.

    Flow:
    1. Validate source file (S3)
    2. Load CSV to PostgreSQL staging
    3. Run dbt staging models
    4. Run dbt intermediate + marts
    5. Run dbt tests
    6. Great Expectations data quality check
    7. Generate QA summary artifact
    8. Notify via Slack on failure
    """

    logger = get_run_logger()
    logger.info("Starting Eligibility Pipeline")

    # Step 1: Validate
    source_info = validate_source_file(s3_bucket, s3_key)

    # Step 2: Load
    load_result = load_csv_to_postgres(source_info, target_schema, target_table)

    # Step 3: dbt staging
    dbt_staging_result = run_dbt_staging()

    # Step 4: dbt marts (depends on staging)
    dbt_marts_result = run_dbt_marts(upstream_tasks=[dbt_staging_result])

    # Step 5: dbt tests
    dbt_test_result = run_dbt_tests(upstream_tasks=[dbt_marts_result])

    # Step 6: GX check
    gx_result = run_gx_check(
        context_path="/opt/prefect/gx",
        batch_path="/opt/prefect/data/eligibility",
    )

    # Step 7: QA summary
    qa_summary = generate_qa_summary(
        dbt_result=dbt_test_result,
        gx_result=gx_result,
        load_result=load_result,
    )

    logger.info("Eligibility Pipeline complete")
    return {"status": "success", "qa_summary": qa_summary}


# ============================================================
# Error Handler — Notify on any failure
# ============================================================

@flow(
    name="Eligibility Pipeline — with error handling",
    description="Wrapper flow with error handling and Slack alerting",
    log_prints=True,
)
def eligibility_pipeline_prod(
    s3_bucket: str = "vital-pipeline-bronze",
    s3_key: str = "eligibility/eligibility_dirty.csv",
):
    try:
        return eligibility_pipeline(s3_bucket, s3_key)
    except Exception as exc:
        logger = get_run_logger()
        logger.error(f"Pipeline failed with exception: {exc}")

        notify_failure({
            "run_id": "local-test",
            "failed_task": "eligibility_pipeline",
            "error": str(exc),
        })

        raise


if __name__ == "__main__":
    # Local test run
    result = eligibility_pipeline_prod()
    print(result)
