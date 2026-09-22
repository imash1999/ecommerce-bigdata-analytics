from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'imash1999',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'rfm_segmentation_dag',
    default_args=default_args,
    description='在 PySpark 上定期运行 RFM 用户细分',
    schedule_interval='0 * * * *',
    catchup=False,
) as dag:

    run_rfm_spark = BashOperator(
        task_id='run_rfm_analysis',
        bash_command='python /opt/airflow/scripts/spark/rfm_analysis.py',
    )

    run_rfm_spark
