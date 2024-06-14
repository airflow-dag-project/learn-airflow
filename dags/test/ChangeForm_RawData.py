from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='change_raw_data_form',
    default_args=default_args,
    description='change raw data date column form from raw_data_test_youngjun table',
    schedule_interval='@daily',
)

def change_form():
    # Connect to Redshift
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    create_table_query = """
    DROP TABLE IF EXISTS yusuyeon678.raw_data;
    CREATE TABLE yusuyeon678.raw_data (
        date TIMESTAMP NOT NULL,
        region_code INT NOT NULL,
        region_name VARCHAR(20) NOT NULL,
        office_code INT NOT NULL,
        office_name VARCHAR(20) NOT NULL,
        dust FLOAT NOT NULL,
        dust_24h FLOAT NOT NULL,
        ultradust FLOAT NOT NULL,
        O3 FLOAT NOT NULL,
        NO2 FLOAT NOT NULL,
        CO FLOAT NOT NULL,
        SO2 FLOAT NOT NULL,
        constraint raw_data_pk PRIMARY KEY (date, region_code, region_name, office_code, office_name)
    );
    """
    redshift_hook.run(create_table_query)
    
    # Query to populate group_by_region_test_suyeon from raw_data_test_youngjun
    sql_query = """
    INSERT INTO yusuyeon678.raw_data (date, region_code, region_name, office_code, office_name, dust, dust_24h, ultradust, O3, NO2, CO, SO2)
    SELECT
        TO_TIMESTAMP(CAST(date AS VARCHAR), 'YYYYMMDDHH24MI') AS date,
        region_code,
        region_name,
        office_code,
        office_name,
        dust,
        dust_24h,
        ultradust,
        O3,
        NO2,
        CO,
        SO2
    FROM yusuyeon678.raw_data_test_youngjun;
    """
    
    # Execute the query
    redshift_hook.run(sql_query)

change_form_task = PythonOperator(
    task_id='change_raw_data_date_column_form',
    python_callable=change_form,
    dag=dag,
)

change_form_task