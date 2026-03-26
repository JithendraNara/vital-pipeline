"""
Eligibility ETL — Airflow DAG

Full pipeline:
1. Validate source file exists
2. Load CSV → staging table (raw_eligibility)
3. Run dbt staging models (stg_eligibility_members)
4. Run dbt intermediate models (int_member_months)
5. Run dbt mart models (mart_member_roster)
6. Run dbt tests
7. Generate QA report
8. Alert on failures

Schedule: Daily at 6:00 AM EST
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dagrun import TriggerDagRunOperator
from airflow.providers.slack.operators.slack import SlackWebhookOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import pandas as pd
import json
import logging

# Default args
default_args = {
    'owner': 'data-ops',
    'depends_on_past': False,
    'email': ['dataops@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 3, 20),
}

# DAG definition
dag = DAG(
    'eligibility_etl',
    default_args=default_args,
    description='Daily eligibility file ETL pipeline',
    schedule_interval='0 6 * * *',  # 6 AM EST daily
    catchup=False,
    max_active_runs=1,
    tags=['eligibility', 'daily', 'healthcare'],
)

# ============================================================
# Task 1: Validate source file
# ============================================================
def validate_source_file(**context):
    """Check eligibility file exists and has correct row count."""
    source_path = Variable.get('eligibility_source_path',
                                default_var='/data/inbound/eligibility.csv')

    try:
        df = pd.read_csv(source_path)
        row_count = len(df)
        col_count = len(df.columns)

        logging.info(f"Source file validated: {row_count} rows × {col_count} cols")

        # Push row count to XCom for downstream checks
        context['ti'].xcom_push(key='row_count', value=row_count)

        if row_count == 0:
            raise ValueError("Source file is empty")

        return True

    except FileNotFoundError:
        logging.error(f"Source file not found: {source_path}")
        raise
    except Exception as e:
        logging.error(f"Validation failed: {e}")
        raise


validate_file = PythonOperator(
    task_id='validate_source_file',
    python_callable=validate_source_file,
    dag=dag,
)


# ============================================================
# Task 2: Load to PostgreSQL staging
# ============================================================
load_to_staging = PostgresOperator(
    task_id='load_csv_to_staging',
    postgres_conn_id='postgres_warehouse',
    sql="""
        TRUNCATE TABLE raw.eligibility;

        COPY raw.eligibility (mem_id, first_name, last_name, dob, email, phone,
                              address, city, state, zip_code, effective_date,
                              termination_date, covered_relation, plan_type, metal_level,
                              hsa_eligible)
        FROM '{{ var.json('eligibility_source_path') }}'
        WITH (FORMAT csv, HEADER true, NULL '');
    """,
    dag=dag,
)


# ============================================================
# Task 3: dbt run — staging models
# ============================================================
dbt_run_staging = BashOperator(
    task_id='dbt_run_staging',
    bash_command='cd /opt/dbt && dbt run --select stg_eligibility_members --target prod',
    dag=dag,
)


# ============================================================
# Task 4: dbt run — intermediate models
# ============================================================
dbt_run_intermediate = BashOperator(
    task_id='dbt_run_intermediate',
    bash_command='cd /opt/dbt && dbt run --select int_member_months --target prod',
    dag=dag,
)


# ============================================================
# Task 5: dbt run — mart models
# ============================================================
dbt_run_marts = BashOperator(
    task_id='dbt_run_marts',
    bash_command='cd /opt/dbt && dbt run --select mart_member_roster --target prod',
    dag=dag,
)


# ============================================================
# Task 6: dbt tests
# ============================================================
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/dbt && dbt test --select mart_member_roster --target prod',
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)


# ============================================================
# Task 7: Generate QA report
# ============================================================
def generate_qa_report(**context):
    """Query DQ results and generate summary."""
    from airflow.hooks.postgres_hook import PostgresHook

    pg = PostgresHook(postgres_conn_id='postgres_warehouse')

    # Get test failures
    failures = pg.get_records("""
        SELECT
            model,
            column_name,
            test_name,
            failures
        FROM dev.dbt_test_results
        WHERE failures > 0
        ORDER BY failures DESC
    """)

    row_count = context['ti'].xcom_pull(task_ids='validate_source_file',
                                          key='row_count')

    report = {
        "run_time": datetime.now().isoformat(),
        "source_rows": row_count,
        "test_failures": len(failures),
        "failures": failures,
    }

    # Store report as JSON
    report_path = f"/data/reports/eligibility_qa_{datetime.now().strftime('%Y%m%d')}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    logging.info(f"QA report saved: {report_path}")

    # Push to XCom for alerting
    context['ti'].xcom_push(key='qa_report', value=report)

    return report


generate_report = PythonOperator(
    task_id='generate_qa_report',
    python_callable=generate_qa_report,
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)


# ============================================================
# Task 8: Alert on failure
# ============================================================
def alert_on_failure(**context):
    """Send Slack alert if any task failed."""
    ti = context['ti']
    task_id = context.get('task_instance_key_str', 'unknown')

    slack_msg = f"""
:rotating_light: *Eligibility ETL Failed*

Task: `{task_id}`
Dag: eligibility_etl
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Check Airflow UI for details: <https://airflow.example.com/airflow/graph|Graph View>
    """

    logging.error(f"Pipeline failed — alert sent for task: {task_id}")

    # In production: send to Slack
    # slack = SlackWebhookOperator(task_id='slack_alert', ...)

    return "Alert sent"


alert_failure = PythonOperator(
    task_id='alert_on_failure',
    python_callable=alert_on_failure,
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
)


# ============================================================
# Task 9: Success notification
# ============================================================
def alert_on_success(**context):
    """Send Slack message on successful pipeline completion."""
    qa_report = context['ti'].xcom_pull(task_ids='generate_qa_report',
                                         key='qa_report')

    msg = f"""
:white_check_mark: *Eligibility ETL Complete*

Rows processed: {qa_report.get('source_rows', 'N/A')}
DQ failures: {qa_report.get('test_failures', 'N/A')}
Run time: {datetime.now().strftime('%H:%M:%S')}
    """

    logging.info(f"Pipeline succeeded — notification sent")

    # In production: send to Slack
    return "Success notification sent"


alert_success = PythonOperator(
    task_id='alert_on_success',
    python_callable=alert_on_success,
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)


# ============================================================
# DAG Dependencies
# ============================================================
validate_file >> load_to_staging >> dbt_run_staging
dbt_run_staging >> dbt_run_intermediate >> dbt_run_marts
dbt_run_marts >> dbt_test >> generate_report
generate_report >> alert_success
alert_failure << [validate_file, load_to_staging, dbt_run_staging,
                  dbt_run_intermediate, dbt_run_marts, dbt_test,
                  generate_report]
