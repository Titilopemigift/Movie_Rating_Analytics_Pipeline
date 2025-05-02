# dags/movie_ratings_pipeline.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'titi',
    'start_date': datetime(2025, 4, 28),
    'retries': 1
}

with DAG(
    dag_id='movie_ratings_pipeline',
    default_args=default_args,
    schedule_interval='@daily',   
    chedule_interval='0 8 * * *',
    catchup=False
) as dag:

    extract = BashOperator(
        task_id='extract',
        bash_command='python /opt/airflow/dags/scripts/extract_files_to_s3.py'
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='python /opt/airflow/dags/scripts/transform_data.py'
    )

    load = BashOperator(
        task_id='load',
        bash_command='python /opt/airflow/dags/scripts/load_data.py'
    )

    extract >> transform >> load
