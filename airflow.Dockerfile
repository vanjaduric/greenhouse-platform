FROM apache/airflow:2.8.1
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    procps \
    default-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    psycopg2-binary==2.9.9 \
    apache-airflow-providers-amazon \
    delta-spark==3.1.0