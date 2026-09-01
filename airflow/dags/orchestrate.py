import pendulum

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from ingestion.ingest import ingest_opportunities
from ingestion.load_snowflake import load_opportunities_to_snowflake

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
    
    gold_ephermeral = BashOperator(
        task_id='gold_ephermeral',
        cwd='/opt/airflow/dbt_transform/grant',
        bash_command='dbt run --select eph_opps'
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

    
    
    ingest_cdc() >> load_to_snowflake() >> source_freshness() >> silver_grant >> gold_ephermeral >>  gold_dimensions_agency >> gold_dimensions_opps >> gold_facts
    
orchestrate_dag = orchestrate()

