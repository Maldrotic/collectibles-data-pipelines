"""
Airflow DAG for orchestrating dbt models and tests for the Collectibles data pipeline, once in Snowflake.

This DAG:
1. Runs dbt source freshness checks.
2. Runs dbt models in dependency order.
3. Runs dbt tests after model completion.
4. Generates dbt documentation.
5. Sends failure emails to the team.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "ryan-gahart",
    "depends_on_past": False,
    "email": ["airflow-alerts@collectibles.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="dbt_collectibles",
    description="Run dbt models and tests for the Collectibles data pipeline",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 12, 23),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "collectibles", "analytics"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps"
    )

    check_source_freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command="dbt source freshness"
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test"
    )
    
    generate_docs = BashOperator(
        task_id="generate_docs",
        bash_command="dbt docs generate"
    )
    
    dbt_deps >> check_source_freshness >> dbt_run >> dbt_test >> generate_docs