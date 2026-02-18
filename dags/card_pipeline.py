from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("카드 거래 데이터 집계 시작!")
    return "success"

with DAG(
    dag_id='card_transaction_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lotte', 'card'],
) as dag:

    extract = PythonOperator(
        task_id='extract_raw_data',
        python_callable=print_hello,
    )

    transform = BashOperator(
        task_id='transform_with_spark',
        bash_command='echo "Spark 집계 처리 완료"',
    )

    load = BashOperator(
        task_id='load_to_datamart',
        bash_command='echo "데이터마트 적재 완료"',
    )

    extract >> transform >> load