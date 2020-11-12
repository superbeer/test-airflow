# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from airflow.hooks.S3_hook import S3Hook as AirflowS3Hook
import io
from airflow.hooks.postgres_hook import PostgresHook
import dateutil.parser
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import csv
import datetime


class PostgreSQLToS3Operator(BaseOperator):
    """
    Executes sql code in a specific PostgreSQL database

    :param postgres_conn_id: reference to a specific PostgreSQL database
    :type postgres_conn_id: string
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql', 'dest_prefix_filename',
                       'string_format', 'file_control_name')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            postgres_conn_id='postgres_default',
            parameters=None,
            autocommit=False,
            bucket_name="",
            dest_prefix_filename="",
            aws_conn_id='aws_default',
            delimiter='\t',
            quoting=csv.QUOTE_MINIMAL,
            string_format=None,
            file_control=False,
            file_control_name=None,
            *args, **kwargs):
        super(PostgreSQLToS3Operator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.bucket_name = bucket_name
        self.dest_prefix_filename = dest_prefix_filename
        self.aws_conn_id = aws_conn_id
        self.delimiter = delimiter
        self.quoting = quoting
        self.sql = sql
        self.string_format = string_format
        self.file_control = file_control
        self.file_control_name = file_control_name

    def pre_execute(self, context):
        if self.string_format and isinstance(self.string_format, dict):
            self.log.info(
                "string_format has been specified. Templating the format now.")
            self.sql = self.sql.format(**self.string_format)
            self.log.info("Formatted SQL: %s", self.sql)

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.log.info('PostgreSQL connection id: %s', self.postgres_conn_id)
        s3_hook = AirflowS3Hook(aws_conn_id=self.aws_conn_id)
        start_date_time = datetime.datetime.utcnow()
        self.start_date = start_date_time
        self.log.info("start_date {}".format(start_date_time))

        s3_hook.load_string(self._query_postgres_to_csv(),
                            self.dest_prefix_filename,
                            self.bucket_name, replace=True)
        end_date_time = datetime.datetime.utcnow()
        self.end_date_time = end_date_time
        self.log.info("end_date {}".format(end_date_time))

        if self.file_control == True:
            s3_hook.load_string(self._gen_file_control(),
                                self.file_control_name,
                                self.bucket_name, replace=True)

    def _gen_file_control(self):
        csv_buffer = io.StringIO()
        header = ["file_name", "number_of_record",
                  "start_extraction_date_time", "finish_extraction_date"]
        writer = csv.writer(
            csv_buffer, delimiter=self.delimiter, quoting=self.quoting)
        writer.writerow(header)
        record = [self.dest_prefix_filename, self.record_count,
                  self.start_date_time, self.end_date_time]
        writer.writerow(record)
        return csv_buffer.getvalue()

    def _query_postgres_to_csv(self):
        """
        Queries mysql and returns a cursor to the results.
        """
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        with postgres.get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(self.sql)
            result = cursor.fetchall()

            if self.file_control == True:
                record_count = len(result)
                self.log.info("row_count ={row_count}".format(
                    row_count=record_count))
                self.record_count = record_count

            csv_buffer = io.StringIO()
            header = [i[0] for i in cursor.description]  # fetch header
            writer = csv.writer(
                csv_buffer, delimiter=self.delimiter, quoting=self.quoting)
            writer.writerow(header)
            for record in result:
                writer.writerow(record)
        return csv_buffer.getvalue()
