import logging
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from couriers_loader import load_couriers_from_api, load_couriers_stg_to_dds
from deliveries_loader import load_deliveries_from_api, load_deliveries_stg_to_dds
from dm_courier_ledger import fill_dm_courier_ledger
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
        dag_id='couriers_api_dag',
        default_args=default_args,
        schedule_interval='0/59 * * * *',
        catchup=False,
        tags=['api', 'couriers'],
) as dag:
    @task(task_id='init_schema')
    def init_schema(**context):
        """Initialize database schema by running init_project_5_ddl.sql"""
        from pathlib import Path
        
        dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                log.info("Starting schema initialization")
                
                # Read and execute the SQL file
                sql_file = Path('/lessons/dags/init_project_5_ddl.sql')
                script = sql_file.read_text()
                cur.execute(script)  # type: ignore
                
                log.info("Schema initialization completed successfully")
        
        return True

    @task(task_id='load_couriers_from_api')
    def load_couriers_task(**context):
        return load_couriers_from_api(**context)


    @task(task_id='load_deliveries_from_api')
    def load_deliveries_task(**context):
        return load_deliveries_from_api(**context)


    @task(task_id='load_couriers_stg_to_dds')
    def load_couriers_stg_to_dds_task(**context):
        return load_couriers_stg_to_dds(**context)


    @task(task_id='load_deliveries_stg_to_dds')
    def load_deliveries_stg_to_dds_task(**context):
        return load_deliveries_stg_to_dds(**context)


    @task(task_id='fill_dm_courier_ledger')
    def fill_dm_courier_ledger_task(**context):
        return fill_dm_courier_ledger(**context)

    # Define task dependencies
    init_task = init_schema()
    couriers_task = load_couriers_task()
    deliveries_task = load_deliveries_task()
    couriers_dds_task = load_couriers_stg_to_dds_task()
    deliveries_dds_task = load_deliveries_stg_to_dds_task()
    cdm_task = fill_dm_courier_ledger_task()

    init_task >> couriers_task >> deliveries_task >> couriers_dds_task >> deliveries_dds_task >> cdm_task
