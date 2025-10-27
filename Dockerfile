FROM apache/airflow:3.1.0

RUN pip install --no-cache-dir \
    apache-airflow \
    apache-airflow-providers-mongo \
    pymongo