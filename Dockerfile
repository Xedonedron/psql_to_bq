FROM apache/airflow:2.9.3
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
RUN apt-get update && apt-get install -y iputils-ping && rm -rf /var/lib/apt/lists/*