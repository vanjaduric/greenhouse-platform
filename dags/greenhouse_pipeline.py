from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import psycopg2
import os

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------

default_args = {
    "owner": "vanja",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="greenhouse_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold every 30 minutes",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["greenhouse", "pipeline"]
) as dag:

    # ── Task 1 — Check Bronze files landed ────────────────────────
    check_bronze = S3KeySensor(
        task_id="check_bronze_landed",
        bucket_name="greenhouse-lake",
        bucket_key="bronze/sensors/facility_id=facility_A/",
        aws_conn_id="minio_s3",
        mode="reschedule",
        poke_interval=30,
        timeout=1800,
        verify=False,
        soft_fail=False
    )

    # ── Task 2 — Run Silver Spark job ─────────────────────────────
    run_silver = BashOperator(
        task_id="run_silver",
        bash_command="""
            cd /opt/airflow/spark_jobs && \
            python silver_job.py
        """,
        env={
            "MINIO_ENDPOINT":   "http://minio:9000",
            "MINIO_ACCESS_KEY": "vadjuric",
            "MINIO_SECRET_KEY": "Wireswires12345.",
            "BRONZE_PATH":      "s3a://greenhouse-lake/bronze/sensors",
            "SILVER_PATH":      "s3a://greenhouse-lake/silver/sensors",
            "CHECKPOINT_PATH":  "s3a://greenhouse-lake/checkpoints/silver",
        }
    )

    # ── Task 3 — Run Gold Spark job ───────────────────────────────
    run_gold = BashOperator(
        task_id="run_gold",
        bash_command="""
            cd /opt/airflow/spark_jobs && \
            python gold_job.py
        """,
        env={
            "MINIO_ENDPOINT":   "http://minio:9000",
            "MINIO_ACCESS_KEY": "vadjuric",
            "MINIO_SECRET_KEY": "Wireswires12345.",
            "SILVER_PATH":      "s3a://greenhouse-lake/silver/sensors",
            "VPD_PATH":         "s3a://greenhouse-lake/silver/vpd",
            "GOLD_PATH":        "s3a://greenhouse-lake/gold",
            "POSTGRES_HOST":    "postgres",
            "POSTGRES_DB":      "greenhouse",
            "POSTGRES_USER":    "postgres",
            "POSTGRES_PASS":    "Wireswires12345.",
        }
    )

    # ── Task 4 — Verify Gold rows written to Postgres ─────────────
    def verify_gold_written(**context):
        """
        Checks that Gold rows were actually written for this execution window.
        Fails the task if nothing landed — alerts you before Grafana shows stale data.
        """
        execution_date = context["execution_date"]
        window_start = execution_date.replace(second=0, microsecond=0)

        conn = psycopg2.connect(
            host="postgres",
            dbname="greenhouse",
            user="postgres",
            password="Wireswires12345."
        )

        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM fct_30min_climate
                WHERE window_start >= %s
                AND gold_processed_at >= NOW() - INTERVAL '10 minutes'
            """, (window_start,))
            count = cur.fetchone()[0]

        conn.close()

        if count == 0:
            raise ValueError(
                f"Gold verification failed — no rows written for window {window_start}. "
                f"Check Gold job logs."
            )

        print(f"Gold verification passed — {count} rows written for {window_start}")
        return count

    verify_gold = PythonOperator(
        task_id="verify_gold",
        python_callable=verify_gold_written,
        provide_context=True
    )

    # ── Task 5 — Log pipeline summary ─────────────────────────────
    def log_summary(**context):
        """Prints a summary of what ran — visible in Airflow task logs."""
        execution_date = context["execution_date"]
        print(f"""
        ══════════════════════════════════════════
        Greenhouse Pipeline — Run Complete
        ══════════════════════════════════════════
        Execution time : {execution_date}
        Tasks completed: check_bronze → silver → gold → verify
        Grafana updated: http://localhost:3000
        MinIO path     : s3a://greenhouse-lake/
        ══════════════════════════════════════════
        """)

    pipeline_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=log_summary,
        provide_context=True
    )

    # ── Task dependencies ─────────────────────────────────────────
    check_bronze >> run_silver >> run_gold >> verify_gold >> pipeline_summary