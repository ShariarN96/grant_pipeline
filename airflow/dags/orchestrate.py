import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from ingestion.ingest import ingest_opportunities
from ingestion.load_snowflake import load_opportunities_to_snowflake
from llm.classifier import run_classification
from llm.news import generate_digest

@dag(
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 8, 31, tz="America/Chicago"),
    catchup=False,
)
def orchestrate():
    @task
    def ingest_cdc():
        return ingest_opportunities()
    
    @task
    def load_to_snowflake():
        return load_opportunities_to_snowflake()
    
    @task.bash
    def source_freshness():
        return "cd /opt/airflow/dbt_transform/grant && dbt source freshness"
    
    silver_grant = BashOperator(
    task_id='silver_grant',
    cwd='/opt/airflow/dbt_transform/grant',
    bash_command='dbt run --select silver_grant'
    )
    
    classify_grants = PythonOperator(
    task_id="classify_grants",
    python_callable=run_classification,
    op_kwargs={"classify_all": False},   
    )
    
    gold_intermediate = BashOperator(
        task_id='gold_intermediate',
        cwd='/opt/airflow/dbt_transform/grant',
        bash_command='dbt run --select int_opps'
    )
    
    gold_dimensions_agency = BashOperator(
        task_id='gold_dimensions_agency',
        cwd='/opt/airflow/dbt_transform/grant',
        bash_command='dbt run --select dim_agency'
    )
    
    gold_dimensions_opps = BashOperator(
        task_id='gold_dimensions_opps',
        cwd='/opt/airflow/dbt_transform/grant',
        bash_command='dbt snapshot'
    )

  
    gold_facts = BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/dbt_transform/grant',
        bash_command='dbt run --select fact'
    )

    @task
    def generate_newsletter():
        return generate_digest(
        days_back=1,
        category=None)

    ingest_cdc() >> load_to_snowflake() >> source_freshness() >> silver_grant >> classify_grants >> gold_intermediate >>  gold_dimensions_agency >> gold_dimensions_opps >> gold_facts >> generate_newsletter()
    
orchestrate_dag = orchestrate()

