FROM apache/airflow:3.0.1

USER airflow
RUN pip install apache-airflow-providers-postgres==5.7.0

USER airflow
