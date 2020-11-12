# -*- coding: utf-8 -*-
"""
Add document here
"""
from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from plugins.operators.postgres_to_s3 import PostgreSQLToS3Operator

import csv

DAG_NAME = 'test_upload_workflow_1'


str_today = "{{ds_nodash}}"

default_args = {
    'owner': 'CTO DLI',
    'depends_on_past': False,
    'email': None,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'description': __doc__,
}


with DAG(dag_id=DAG_NAME,
         default_args=default_args,
         start_date=datetime(2020, 11, 10),
         catchup=False,
         max_active_runs=1,
         concurrency=1,
         schedule_interval="0 1 * * *") as main_dag:

    start_task = (DummyOperator(
        task_id='start_task'
    ))
    end_task = (DummyOperator(
        task_id='end_task'
    ))

    sql = "SELECT calendar FROM bi_report.calendar limit 10;"
    postgres_conn_id = "bi_report_id"
    aws_conn_id = "aws_datalake_conn"
    bucket_name = "cdl.cto.prod"
    dest_prefix_filename = "airflow/result/test_upload.csv"
    postresql_upload_s3_task = (PostgreSQLToS3Operator(
        task_id="upload",
        aws_conn_id=aws_conn_id,
        postgres_conn_id=postgres_conn_id,
        bucket_name=bucket_name,
        dest_prefix_filename=dest_prefix_filename,
        sql=sql,
        delimiter='|'
    ))

    start_task >> postresql_upload_s3_task >> end_task
