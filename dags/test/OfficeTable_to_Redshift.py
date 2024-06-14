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
    dag_id='group_by_office_elt',
    default_args=default_args,
    description='Populate group_by_office table from raw_data_test_youngjun table',
    schedule_interval='@daily',
)

def populate_group_by_office():
    # Connect to Redshift
    redshift_hook = PostgresHook(postgres_conn_id='redshift_test_dev')
    
    create_table_query = """
    DROP TABLE IF EXISTS yusuyeon678.group_by_office;
    CREATE TABLE yusuyeon678.group_by_office (
        date TIMESTAMP NOT NULL,
        office_code INT NOT NULL,
        office_name VARCHAR(20) NOT NULL,
        dust FLOAT NOT NULL,
        ultradust FLOAT NOT NULL,
        O3 FLOAT NOT NULL,
        NO2 FLOAT NOT NULL,
        CO FLOAT NOT NULL,
        SO2 FLOAT NOT NULL,
        CONSTRAINT group_by_office_pk PRIMARY KEY (date, office_code)
    );
    """
    redshift_hook.run(create_table_query)
    
    # Query to populate group_by_office_test_suyeon from raw_data_test_youngjun
    sql_query = """
    INSERT INTO yusuyeon678.group_by_office (date, office_code, office_name, dust, ultradust, O3, NO2, CO, SO2)
    SELECT
        date,
        office_code,
        office_name,
        ROUND(AVG(dust), 2) AS dust,
        ROUND(AVG(ultradust), 2) AS ultradust,
        ROUND(AVG(o3), 2) AS o3,
        ROUND(AVG(no2), 2) AS no2,
        ROUND(AVG(co), 2) AS co,
        ROUND(AVG(so2), 3) AS so2
    FROM yusuyeon678.raw_data
    GROUP BY date, office_code, office_name;
    """
    
    # Execute the query
    redshift_hook.run(sql_query)

populate_task = PythonOperator(
    task_id='populate_group_by_office_task',
    python_callable=populate_group_by_office,
    dag=dag,
)

populate_task