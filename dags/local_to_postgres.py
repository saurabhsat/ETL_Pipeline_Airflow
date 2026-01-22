from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import os

# -------------------------------
# Config
# -------------------------------
CSV_FILE = "/opt/airflow/data/employee_attrition.csv"
TABLE_NAME = "employee_attrition"
POSTGRES_CONN = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

# -------------------------------
# Functions
# -------------------------------
def load_csv_to_postgres():
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"{CSV_FILE} not found!")

    # Read CSV
    df = pd.read_csv(CSV_FILE)

    # Connect to Postgres
    engine = create_engine(POSTGRES_CONN)
    with engine.begin() as conn:
        # Create table if not exists
        columns = ", ".join([f'"{col}" TEXT' for col in df.columns])
        create_sql = f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns});'
        conn.execute(text(create_sql))

        # Insert data
        df.to_sql(TABLE_NAME, con=conn, if_exists="replace", index=False)

    print(f"{len(df)} rows loaded into {TABLE_NAME}")

# -------------------------------
# DAG definition
# -------------------------------
with DAG(
    dag_id="local_csv_to_postgres",
    start_date=datetime(2026, 1, 22),
    schedule_interval=None,
    catchup=False,
    tags=["example"],
) as dag:

    load_task = PythonOperator(
        task_id="load_csv_to_postgres",
        python_callable=load_csv_to_postgres,
    )
