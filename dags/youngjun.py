from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
import requests
import datetime
import os

import pdb

# DAG �⺻ ����
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG ����
with DAG(
    'api_to_s3_to_redshift',
    default_args=default_args,
    description='Fetch data from API, upload to S3, and load into Redshift',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # 1. API ȣ���Ͽ� ������������ ���·� ��ȯ�ϴ� �Լ�
    def fetch_api_data():
        # ���� ��¥�� YYYYMMDDHH �������� ��� (��: 1�ð� �� ������ ��û)
        current_datetime = (datetime.datetime.now() - datetime.timedelta(hours=1)).strftime("%Y%m%d%H00")
        
        # API URL (json �������� ��û)
        api_url = f"http://openapi.seoul.go.kr:8088/4b746a725579756e35386955445a73/json/TimeAverageCityAir/1/100/{current_datetime}"
        response = requests.get(api_url)
        try:
            response.raise_for_status()  # HTTP ���� ���� �ڵ� Ȯ��
            data = response.json()
        except requests.exceptions.HTTPError as http_err:
            raise ValueError(f"HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as req_err:
            raise ValueError(f"Request exception occurred: {req_err}")
        except ValueError as json_err:
            raise ValueError(f"JSON decode error: {json_err}")
        
        if "TimeAverageCityAir" not in data or "row" not in data["TimeAverageCityAir"]:
            raise ValueError("No data returned from API")
        
        items = data["TimeAverageCityAir"]["row"]
        if not items:
            raise ValueError("No data available for the requested date and time.")
        
        df = pd.DataFrame(items)

        # �÷����� ERD�� ���� �̸����� ����
        df.columns = [
            'date', 'region_code', 'region_name', 'office_code', 'office_name',
            'dust_1h', 'dust_24h', 'ultradust', 'O3', 'NO2', 'CO', 'SO2'
        ]
        # �������������� UTF-8 ���ڵ����� CSV ������ ���ڿ��� ��ȯ
        csv_data = df.to_csv(index=False, encoding='utf-8-sig')
        
        # pdb.set_trace()
        
        # ���� �۾� ���丮�� ����Ͽ� ���� ����
        file_path = os.path.join(os.getcwd(), 'api_data.csv')
        with open(file_path, 'w', encoding='utf-8-sig') as f:
            f.write(csv_data)
        
        return file_path

    fetch_data = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )

    # 2. ������������ ���·� ��ȯ�� �����͸� S3�� ���ε�
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        filename="{{ task_instance.xcom_pull(task_ids='fetch_api_data') }}",
        dest_bucket='dust-dag',
        dest_key='dataSource/api_data.csv',
        aws_conn_id='aws_s3',
        replace=True  # ������ �̹� �����ϴ� ��� �����
    )

    # 3. S3 �����͸� Redshift ���̺� ����
    load_to_redshift = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        schema='yusuyeon678',
        table='raw_data_test_youngjun',
        s3_bucket='dust-dag',
        s3_key='dataSource/api_data.csv',
        copy_options=['csv', 'IGNOREHEADER 1'],
        redshift_conn_id='redshift_test_dev',
        aws_conn_id='aws_s3',
    )

    # Task ���� ����
    fetch_data >> upload_to_s3 >> load_to_redshift