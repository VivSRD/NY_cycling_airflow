from datetime import datetime, timedelta
from multiprocessing.spawn import old_main_modules

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from clickhouse_driver import Client
from dotenv import load_dotenv
from io import BytesIO

import pandas as pd
import requests, zipfile
import tempfile
import os


load_dotenv()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


BUCKET_KEY = os.getenv('BUCKET_KEY')
BUCKET_NAME = os.getenv('BUCKET_NAME')
FILE_NAME = os.getenv('FILE_NAME')
URL = os.getenv('URL')



def unzip_file():
    '''
    Get zip archive from public s3 bucket and extracts the inner csv file;
    Out: FILE_NAME (csv file)
    '''
 
    req = requests.get(URL, stream=True)
    zip_file = zipfile.ZipFile(BytesIO(req.content))
    csv = zip_file.extract(FILE_NAME)
    return csv


def load_data():
    '''
    Transfers data from csv file to Clickhouse table 
    '''

    csv_file = unzip_file()
    client = Client('localhost', settings={
        'use_numpy': True,
        'input_format_skip_unknown_fields': True
        }
    )
    client.execute('DROP TABLE IF EXISTS tripdata')
    data = pd.read_csv(
        csv_file, 
        skip_blank_lines=True, 
        usecols=['tripduration', 'starttime', 'gender']
    )
    client.execute(
    """
        CREATE TABLE tripdata (
            `tripduration` Int64,
            `starttime` DateTime64,
            `gender` Int64
        )
        ENGINE = MergeTree() 
        ORDER BY `starttime`
    """
    )

    client.insert_dataframe('INSERT INTO tripdata VALUES', data)


def daytrips_report(**context):
    '''
    Excecute query to generate the first report; 
    formated Clickhouse answer pushes to XCom;
    the result is written to csv file
    '''

    client = Client('localhost', settings={
        'use_numpy': True
        }
    )
    res = client.query_dataframe(
    """
        SELECT 
            toDate(starttime) AS Date,
            count() AS Day_trips
        FROM default.tripdata
        WHERE toDate(starttime) = '2019-01-01'
        GROUP BY Date
    """,
    )
    res.to_csv('daytrips_report.csv', index=False)

    # push result to XCom
    context['ti'].xcom_push(
        key='result', 
        value=f'Date: {res.iloc[0, 0]} - {res.iloc[0, 1]} trips'
    )
 
    
    
def avg_duration_report(**context):
    '''
    Excecute query to generate the second report; 
    formated Clickhouse answer pushes to XCom;
    the result is written to csv file
    '''

    client = Client('localhost', settings={
        'use_numpy': True
        }
    )
    res = client.query_dataframe(
    """
        SELECT 
            toDate(starttime) AS Date,
            count() AS Day_trips
        FROM default.tripdata
        WHERE toDate(starttime) = '2019-01-01'
        GROUP BY Date
    """,
    )
    res.to_csv('avg_duration_report.csv', index=False)

    # push result to XCom
    context['ti'].xcom_push(
        key='result', 
        value=f'Date: {res.iloc[0, 0]} - avg_duration: {res.iloc[0, 1]} km'
    )


def daytrips_per_gender_report(**context):
    '''
    Excecute query to generate the third report; 
    formated Clickhouse answer pushes to XCom;
    the result is written to csv file
    '''

    client = Client('localhost', settings={
        'use_numpy': True
        }
    )
    res = client.query_dataframe(
    """
        SELECT
            toDate(starttime) AS Date,
            count(gender) AS g_cnt,
            gender,
            round((g_cnt / (
                SELECT count()
                FROM default.tripdata
                WHERE toDate(starttime) = '2019-01-01'
            )) * 100, 2) AS `Percentage, %`
        FROM tripdata
        WHERE toDate(starttime) = '2019-01-01'
        GROUP BY
            Date,
            gender
        ORDER BY g_cnt DESC
    """,
    )
    res.to_csv('daytrips_per_gender_report.csv', index=False)

    # push result to XCom
    context['ti'].xcom_push(
        key='result', 
        value=f'Date: {res.iloc[0, 0]} \n \
        Gender {res.iloc[0, 2]} - {res.iloc[0, 3]}% \n \
        Gender {res.iloc[1, 2]} - {res.iloc[1, 3]}%  \n \
        Gender {res.iloc[2, 2]} - {res.iloc[2, 3]}%'
    )


with DAG(
    dag_id="NY_cycling_dag",
    default_args=default_args,
    schedule_interval="@daily",
    tags=['transfertripdata']
) as dag:


    new_file_onto_S3_trigger = S3KeySensor(
            task_id="new_file_onto_S3_trigger",
            bucket_name="tripdata",
            bucket_key=BUCKET_KEY,
            aws_conn_id="aws_s3_airflow_user"
        )

    unziped_files_from_s3 = PythonOperator(
        task_id="unziped_files_from_s3",
        python_callable=unzip_file
    )
    
    load_data_to_Clickhouse = PythonOperator(
        task_id="load_data_to_Clickhouse",
        python_callable=load_data
    )

    daytrips_report = PythonOperator(
        task_id='daytrips_report',
        provide_context=True,
        python_callable=daytrips_report
    )

    avg_duration_report = PythonOperator(
        task_id='avg_duration_report',
        provide_context=True,
        python_callable=avg_duration_report
    )

    daytrips_per_gender_report = PythonOperator(
        task_id='daytrips_per_gender_report',
        provide_context=True,
        python_callable=daytrips_per_gender_report
    )


    new_file_onto_S3_trigger >> unziped_files_from_s3 >> load_data_to_Clickhouse
    load_data_to_Clickhouse >> daytrips_report >> avg_duration_report
    avg_duration_report >> daytrips_per_gender_report 
