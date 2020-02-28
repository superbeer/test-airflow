from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from plugins.operators.my_custom_operator import MyCustomOperator
from datetime import datetime


with DAG('dag_with_custom_operator', start_date=datetime(2020, 2, 28)) as dag:
    (
        BashOperator(
            task_id='bash_hello',
            bash_command='echo "HELLO!"'
        )
        >> MyCustomOperator(
            task_id='task_with_custom_operator'
        )
    )
