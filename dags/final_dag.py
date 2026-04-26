from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def notify_failure(context):
    print(f"DAG failed! Task: {context['task_instance'].task_id}")


default_args = {
    "owner": "admin",
    "email": ["test@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="final_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["etl", "daily"],
) as dag:
    start_task = PythonOperator(
        task_id="start_pipeline", python_callable=lambda: print("start pipeline")
    )

    def push_value():
        return "DataEngineer_2026"

    produce_data = PythonOperator(task_id="produce_data", python_callable=push_value)

    def check_weekday():
        return datetime.now().weekday() < 5

    check_day = ShortCircuitOperator(
        task_id="check_if_weekday", python_callable=check_weekday
    )

    process_data = BashOperator(
        task_id="process_data", bash_command='echo "data processing: {{ ds }}"'
    )

    print_date = BashOperator(
        task_id="print_execution_date",
        bash_command='echo "Execution date is: {{ ds }}"',
    )

    read_xcom = PythonOperator(
        task_id="read_xcom_value",
        python_callable=lambda ti: print(
            f"Received: {ti.xcom_pull(task_ids='produce_data')}"
        ),
    )

    final_task = PythonOperator(
        task_id="final_task",
        python_callable=lambda: print("pipeline finished"),
        trigger_rule="all_done",
    )

    (
        start_task
        >> produce_data
        >> check_day
        >> process_data
        >> [print_date, read_xcom]
        >> final_task
    )
