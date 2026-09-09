ARG AIRFLOW_BASE_IMAGE=apache/airflow:2.9.3-python3.11
FROM ${AIRFLOW_BASE_IMAGE}

USER root
# Java is required for pyspark's bundled spark-submit/JVM to run.
# Docker CLI is required because BashOperators will eventually run
# `docker exec ...` against the host daemon via the mounted
# /var/run/docker.sock — without the client binary installed here,
# those tasks would fail with "docker: command not found".
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps ca-certificates curl gnupg && \
    install -m 0755 -d /etc/apt/keyrings && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg && \
    chmod a+r /etc/apt/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" \
      > /etc/apt/sources.list.d/docker.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends docker-ce-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --default-timeout=180 --retries 8 -r /requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/default-java
# pyspark pip package ships its own spark-submit; expose it on PATH
ENV SPARK_HOME=/home/airflow/.local/lib/python3.11/site-packages/pyspark
ENV PATH=$SPARK_HOME/bin:$PATH
