FROM apache/airflow:2.10.2-python3.12

USER root
RUN apt-get update && apt-get install -y \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/* # need this because psycopg2 uses pg_hba.conf 

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

