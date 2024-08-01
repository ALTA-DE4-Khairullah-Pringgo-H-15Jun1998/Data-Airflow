from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

dag = DAG(
        'khairullah_airflow_task1',
        description='Solusi task1 data airflow',
        schedule_interval='0 */5 * * *',
        start_date=datetime(2023, 1, 1),
        catchup=False
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

def push_xcom(**kwargs):
    value = 'nilai_saya'
    kwargs['ti'].xcom_push(key='solusi_saya', value=value)

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_xcom,
    provide_context=True,
    dag=dag,
)

def pull_xcoms(**kwargs):
    ti = kwargs['ti']
    value1 = ti.xcom_pull(task_ids='push_task1', key='kunci1')
    value2 = ti.xcom_pull(task_ids='push_task2', key='kunci2')
    print(f'Nilai yang diambil: {value1}, {value2}')

pull_task = PythonOperator(
    task_id='pull_task',
    provide_context=True,
    python_callable=pull_xcoms,
    dag=dag,
)

start >> push_task >> pull_task
