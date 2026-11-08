# One image for every Python/Spark service in the stack:
#   airflow-webserver, airflow-scheduler, airflow-init, spark-master, spark-worker, jupyter
#
# Using the same image for the Spark cluster and the Airflow scheduler (which acts as the
# spark-submit client / driver) guarantees that the Spark version, Java version and Python
# version are identical on the driver and executors -- PySpark refuses to run when the
# driver and worker Python minor versions differ.
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.10
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root
# Java 17 is required by Spark 3.5. procps provides `ps`, which Spark's scripts use.
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/java-17 \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# pip installs PySpark (which bundles the full Spark 3.5 runtime) into the airflow user's site-packages.
ENV SPARK_HOME=/home/airflow/.local/lib/python${PYTHON_VERSION}/site-packages/pyspark \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    PYTHONUNBUFFERED=1
