FROM apache/airflow:3.1.8

USER root

# Cài Java để PySpark chạy được trong container Airflow
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Khai báo JAVA_HOME và đưa java vào PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Cài thêm Python packages của project
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt