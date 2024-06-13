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
    dag_id='seoul_ultradust_summary_elt',
    default_args=default_args,
    description='Populate seoul_ultradust_summary table from raw_data_test_youngjun table',
    schedule_interval='@daily',
)

def populate_seoul_ultradust_summary():
    # Connect to Redshift
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    create_table_query = """
    DROP TABLE IF EXISTS yusuyeon678.seoul_ultradust_summary;
    CREATE TABLE yusuyeon678.seoul_ultradust_summary (
        date TIMESTAMP primary key,
        ultradust FLOAT NOT NULL
    );
    """
    redshift_hook.run(create_table_query)
    
    # Query to populate seoul_ultradust_summary_test_suyeon from raw_data_test_youngjun
    sql_query = """
    INSERT INTO yusuyeon678.seoul_ultradust_summary (date, ultradust)
    SELECT
        date,
        ROUND(AVG(ultradust), 2) AS ultradust
    FROM yusuyeon678.raw_data
    GROUP BY date;
    """
    
    # Execute the query
    redshift_hook.run(sql_query)

populate_task = PythonOperator(
    task_id='populate_seoul_ultradust_summary_task',
    python_callable=populate_seoul_ultradust_summary,
    dag=dag,
)

populate_task