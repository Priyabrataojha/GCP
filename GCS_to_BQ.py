from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define the start date as yesterday
yesterday = datetime.combine(datetime.today() - timedelta(days=1), datetime.min.time())

# Default arguments for the DAG
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id='GCS_TO_BQ',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    description='Load CSV from GCS to BigQuery'
) as dag:

    # Start dummy task
    start = DummyOperator(task_id='Start')

    # Task to load CSV from GCS to BigQuery
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='GCS_to_BQ',
        bucket='project-data-etl-bucket-dev',
        source_objects=['employee_data/dummy_employee_data.csv'],
        source_format='CSV',
        destination_project_dataset_table='project-data-etl.employee.Employe_Data_Project1',
        schema_fields=[
            {"name": "Name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Employee_ID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Department", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Phone_Number", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Joining_Date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Address", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Date_of_Birth", "type": "STRING", "mode": "NULLABLE"},
            {"name": "National_ID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Password", "type": "STRING", "mode": "NULLABLE"}
        ],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id='google_cloud_default'
    )

    # End dummy task
    end = DummyOperator(task_id='End')

    # Define task dependencies
    start >> gcs_to_bq >> end
