from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from io import BytesIO

import boto3
import zipfile
import os
from dotenv import load_dotenv


load_dotenv()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 4, 1),
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

BUCKET_KEY = os.getenv('bucket_key')
BUCKET_KEY_FOR_JC = os.getenv('bucket_key_for_JC')


def unzip():
    bucket = 'tripdata'

    s3_resource = boto3.resource('s3')

    zip_obj = s3_resource.Object(bucket_name=bucket, key=BUCKET_KEY)
    zip_obj_JC = s3_resource.Object(bucket_name=bucket, key=BUCKET_KEY_FOR_JC)
    print(zip_obj, zip_obj_JC)
    buffer = BytesIO(zip_obj.get()["Body"].read())
    buffer_JC = BytesIO(zip_obj_JC.get()["Body"].read())

    z = zipfile.ZipFile(buffer)
    for filename in z.namelist():
        s3_resource.meta.client.upload_fileobj(
            z.open(filename),
            Bucket=bucket,
            Key=filename
        )

    z_JC = zipfile.ZipFile(buffer_JC)
    for filename in z_JC.namelist():
        s3_resource.meta.client.upload_fileobj(
            z_JC.open(filename),
            Bucket=bucket,
            Key=filename
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

    new_JC_file_onto_S3_trigger = S3KeySensor(
            task_id="new_JC_file_onto_S3_trigger",
            bucket_name="tripdata",
            bucket_key=BUCKET_KEY_FOR_JC,
            aws_conn_id="aws_s3_airflow_user"
        )

    unziped_files_to_s3 = PythonOperator(
        task_id="unziped_files_to_s3",
        python_callable=unzip
    )

    ClickHouse_transform = ClickHouseOperator(
        task_id='ClickHouse_transform',
        database='default',
        sql=(
            '''              
                CREATE TABLE IF NOT EXISTS tripdata2(
                    `"tripduration"` Int32,
                    `"starttime"` DateTime64,
                    `"stoptime"` DateTime64,
                    `"start station id"` String,
                    `"start station name"` String,
                    `"start station latitude"` Float,
                    `"start station longitude"` Float,
                    `"end station id"` Int32,
                    `"end station name"` String,
                    `"end station latitude"` Float,
                    `"end station longitude"` Float,
                    `"bikeid"` Int32,
                    `"usertype"` String,
                    `"birth year"` Int8,
                    `"gender"` String
                )
                ENGINE=s3('https://s3.amazonaws.com/tripdata/{{ ds.format('%Y%m') }}-\
                          citibike-tripdata.csv', 'CSV');


                INSERT INTO tripdata
                SELECT *
                FROM S3('https://s3.amazonaws.com/tripdata/JC-{{ ds.format('%Y%m') }}-\
                        citibike-tripdata.csv', 'CSV')

            ''', '''
                INSERT INTO FUNCTION s3('https://s3.amazonaws.com/reports/daytrips_{{ ds }}.csv', \
                                        'CSV', 'Date Date, CNT UInt32')
                SELECT 
                    toDate(starttime) AS Date,
                    count() AS Day_trips
                FROM tripdata2
                WHERE toDate(starttime) = '2019-01-01'
                GROUP BY Date

            ''', '''
                INSERT INTO FUNCTION s3('https://s3.amazonaws.com/reports/avg_duration_{{ ds }}.csv', \
                                        'CSV', 'Date Date, AVG_duration UInt32')
                SELECT
                    toDate(starttime) AS Date,
                    round(AVG(tripduration)) AS AVG_duration
                FROM tripdata2
                WHERE toDate(starttime) = '2019-01-01'
                GROUP BY Date

            ''', '''
                INSERT INTO FUNCTION s3('https://s3.amazonaws.com/reports/avg_duration_{{ ds }}.csv', 'CSV',
                                            'Date Date,
                                            g_cnt UInt32,
                                            gender String,
                                            `Percentage, %` UInt32'
                                        )
                SELECT
                    toDate(starttime) AS Date,
                    count(gender) AS g_cnt,
                    gender,
                    round((g_cnt / (
                        SELECT count()
                        FROM tripdata2
                        WHERE toDate(starttime) = '2019-01-01'
                    )) * 100, 2) AS `Percentage, %`
                FROM tripdata2
                WHERE toDate(starttime) = '2019-01-01'
                GROUP BY
                    Date,
                    gender
                ORDER BY g_cnt DESC

            ''',
        ),
        clickhouse_conn_id='clickhouse_airflow',
    )

    report_aggregate = PythonOperator(
        task_id='report_aggregate',
        provide_context=True,
        python_callable=lambda task_instance, **_:
            print(task_instance.xcom_pull(task_ids='ClickHouse_transform')),
    )

    new_file_onto_S3_trigger >> unziped_files_to_s3
    new_JC_file_onto_S3_trigger >> unziped_files_to_s3

    unziped_files_to_s3 >> ClickHouse_transform >> report_aggregate
