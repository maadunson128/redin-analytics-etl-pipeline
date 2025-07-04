from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import pandas as pd

#creating DAG
with DAG(
    dag_id="redin_analytics_dag",
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    start_date=datetime(2025, 7, 4),
    schedule='@daily' #scheduling daily

) as dag:
    extract_redfin_data = PythonOperator(
        task_id="extract_data_refin",
        python_callable=extract_data  #python function to write and update
    )

    transform_load_data = PythonOperator(
        task_id="transform_raw_data_load_s3",
        python_callable=transform_data  #python function to write and update
    )

    load_raw_s3 = BashOperator(
        task_id="load_raw_data_s3",
        bash_command="path_to_script.sh"  # script to write and update here
    )


    extract_redfin_data >> transform_load_data >> load_raw_s3

