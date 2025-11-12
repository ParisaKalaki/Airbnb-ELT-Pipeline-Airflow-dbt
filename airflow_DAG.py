from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
import pandas as pd
from google.cloud import storage
import io

# ------------------------------
# Configuration
# ------------------------------
BUCKET_NAME = "us-central1-bde-b4b091bc-bucket"
MONTHS = ["05_2020", "06_2020", "07_2020", "08_2020", "09_2020", "10_2020", "11_2020", "12_2020",
          "01_2021", "02_2021", "03_2021"] 
AIRBNB_TABLE = "bronze.airbnb_raw"
POSTGRES_CONN_ID = 'airflow_db' 

# Static files that need to be loaded once
STATIC_FILES = {
    'census_g01_raw': 'data/Census LGA/2016Census_G01_NSW_LGA.csv',
    'census_g02_raw': 'data/Census LGA/2016Census_G02_NSW_LGA.csv',
    'lga_code': 'data/NSW_LGA/NSW_LGA_CODE.csv',
    'lga_suburb': 'data/NSW_LGA/NSW_LGA_SUBURB.csv'
}

TABLES_TO_TRUNCATE = [
    "bronze.airbnb_raw",
    "bronze.census_g01_raw",
    "bronze.census_g02_raw",
    "bronze.lga_code",
    "bronze.lga_suburb"
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ------------------------------
# Functions
# ------------------------------
def truncate_tables():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    for t in TABLES_TO_TRUNCATE:
        cursor.execute(f"TRUNCATE TABLE {t} CASCADE;")
    conn.commit()
    cursor.close()
    conn.close()
    print("All tables truncated.")

def normalize_date(value):
    if isinstance(value, str) and '/' in value:
        try:
            return datetime.strptime(value.strip(), "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return value

def load_csv_to_postgres(file_key, table_name):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_key)
    content = blob.download_as_text()
    df = pd.read_csv(io.StringIO(content))

    # Remove unwanted columns starting with 'Unnamed:'
    cols_to_keep = [col for col in df.columns if not col.lower().startswith('unnamed:')]
    df = df[cols_to_keep]

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cols = ', '.join([str(c).lower() for c in df.columns])
    placeholders = ', '.join(['%s'] * len(df.columns))
    sql_insert = f'INSERT INTO {table_name} ({cols}) VALUES ({placeholders});'

    for _, row in df.iterrows():
        clean_row = [
            normalize_date(v) if pd.notnull(v) else None
            for v in row
        ]
        cursor.execute(sql_insert, clean_row)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{file_key} loaded into {table_name}")

# ------------------------------
# Define DAG
# ------------------------------
with DAG(
    dag_id='airbnb_dbt_pipeline_manual',
    default_args=default_args,
    description='Airflow DAG for loading Airbnb data month-by-month, dbt Cloud runs manually',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1
) as dag:

    # 1️⃣ Truncate tables
    truncate_task = PythonOperator(
        task_id='truncate_tables',
        python_callable=truncate_tables,
    )

    # 2️⃣ Load static lookup files
    static_load_tasks = []
    for table_name, file_path in STATIC_FILES.items():
        task = PythonOperator(
            task_id=f'load_{table_name}',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'file_key': file_path,
                'table_name': f'bronze.{table_name}'
            },
        )
        static_load_tasks.append(task)
    truncate_task >> static_load_tasks

    # 3️⃣ Load monthly Airbnb files sequentially
    previous_task = static_load_tasks  # first monthly load waits for all static loads

    for month in MONTHS:
        load_task = PythonOperator(
            task_id=f'load_airbnb_{month}',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'file_key': f'data/listings/{month}.csv',
                'table_name': AIRBNB_TABLE
            },
        )
        # Sequential dependency: current month waits for previous
        chain(previous_task, load_task)
        previous_task = load_task

    # 4️⃣ Placeholder task for manual dbt Cloud run
    manual_dbt_run = DummyOperator(
        task_id='manual_dbt_cloud_run'
    )
    previous_task >> manual_dbt_run
