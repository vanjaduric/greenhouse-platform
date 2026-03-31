from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from config import silver_env, gold_env

default_args = {
    "owner": "vanja",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="greenhouse_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold every 30 minutes",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["greenhouse", "pipeline"]
) as dag:

    check_bronze = S3KeySensor(
        task_id="check_bronze_landed",
        bucket_name="greenhouse-lake",
        bucket_key="bronze/sensors/facility_id=facility_A/date={{ data_interval_end.strftime('%Y-%m-%d') }}/hour={{ data_interval_end.hour }}/*",
        aws_conn_id="minio_s3",
        mode="reschedule",
        poke_interval=30,
        timeout=1800,
        verify=False,
        wildcard_match=True
    )

    run_silver = BashOperator(
        task_id="run_silver",
        bash_command="cd /opt/airflow/spark_jobs && python silver_job.py",
        env={
            "EXECUTION_DATE": "{{ data_interval_end.isoformat() }}",
            **silver_env
        }
    )

    run_gold = BashOperator(
        task_id="run_gold",
        bash_command="cd /opt/airflow/spark_jobs && python gold_job.py",
        env={
            "EXECUTION_DATE": "{{ data_interval_end.isoformat() }}",
            **gold_env
        }
    )

    check_bronze >> run_silver >> run_gold