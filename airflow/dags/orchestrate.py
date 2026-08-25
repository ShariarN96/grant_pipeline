from airflow.sdk import dag, task
from ingestion.ingest import ingest_opportunities

@dag
def orchestrate():
    @task
    def ingest_cdc():
        return ingest_opportunities()
    
    # @task.bash
    # def clean_target():
    #     return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"
    
    @task.bash
    def source_freshness():
        return "cd /opt/airflow/dbt_transform/grant && dbt source freshness"

    ingest_cdc() >> source_freshness()
    
orchestrate_dag = orchestrate()

