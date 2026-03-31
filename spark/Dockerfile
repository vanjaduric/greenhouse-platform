FROM apache/airflow:2.8.1
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    procps \
    default-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir --timeout 300 \
    pyspark==3.5.1 \
    delta-spark==3.1.0 \
    boto3==1.34.0 \
    psycopg2-binary==2.9.9

COPY --chown=airflow:airflow . /app
WORKDIR /app

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ENTRYPOINT []
CMD ["python3", "bronze_job.py"]