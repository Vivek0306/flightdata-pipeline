FROM apache/airflow:2.8.1

USER root

# install java 17 for PySpark
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# install Python packages
RUN pip install \
    google-cloud-bigquery \
    google-cloud-storage \
    pyspark==3.5.1 \
    python-dotenv \
    pytz