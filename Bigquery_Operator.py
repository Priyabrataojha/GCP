import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

yesterday = datetime.combine(datetime.today() - timedelta(days=1), datetime.min.time())
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

project_id = 'project-data-etl'
dataset = 'employee'
source_table = f'{project_id}.{dataset}.Employe_Data_Project1'
dest_table = f'{project_id}.{dataset}.Employe_Data_Project_New'

sqlquery = f'''
    CREATE OR REPLACE TABLE `{dest_table}` AS 
    SELECT * FROM `{source_table}`
'''

with DAG(
    dag_id='BQ_EXTRACT_CREATE_TABLE',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='Start'
    )

    # Uncomment and configure this when ready to use BigQuery
    BQExtractandLoad = BigQueryInsertJobOperator(
        task_id='Run_BQ_Extract',
        configuration={
            "query": {
                'query': sqlquery,
                'useLegacySql': False,
            }
        }
    )

    end = DummyOperator(
        task_id='End'
    )

    start >> BQExtractandLoad >> end
    # start >> end
