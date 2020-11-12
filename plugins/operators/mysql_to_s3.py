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
from datetime import timedelta
from airflow.hooks.mysql_hook import MySqlHook
from airflow.contrib.hooks.aws_hook import AwsHook
import dateutil.parser
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import io
import csv
from string import Template
import datetime


class MySQLToS3Operator(BaseOperator):
    """
    Executes sql code in a specific MySQL database

    :param mysql_conn_id: reference to a specific mysql database
    :type mysql_conn_id: string
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param database: name of database which overwrite defined one in connection
    :type database: string
    """

    template_fields = ('sql', 'dest_prefix_filename')
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            sql,
            mysql_conn_id='mysql_default',
            parameters=None,
            autocommit=False,
            bucket_name="",
            dest_prefix_filename="",
            aws_conn_id='aws_default',
            delimiter='\t',
            quoting=csv.QUOTE_MINIMAL,
            *args, **kwargs):
        super(MySQLToS3Operator, self).__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.bucket_name = bucket_name
        self.dest_prefix_filename = dest_prefix_filename
        self.aws_conn_id = aws_conn_id
        self.delimiter = delimiter
        self.quoting = quoting

    # QUOTE_ALL

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.save_to_s3()

    def _query_mysql_tocsv(self):
        """
        Queries mysql and returns a cursor to the results.
        """
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = mysql.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        result = cursor.fetchall()
        csv_buffer = io.StringIO()
        header = [i[0] for i in cursor.description]  # fetch header
        writer = csv.writer(
            csv_buffer, delimiter=self.delimiter, quoting=self.quoting)
        writer.writerow(header)
        for record in result:
            writer.writerow(record)
        return csv_buffer.getvalue()

    def save_to_s3(self):
        s3_hook = AirflowS3Hook(aws_conn_id=self.aws_conn_id)
        s3_hook.load_string(self._query_mysql_tocsv(),
                            self.dest_prefix_filename,
                            self.bucket_name, replace=True)
