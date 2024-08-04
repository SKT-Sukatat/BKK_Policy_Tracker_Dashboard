from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'User',
    'email':['email']
}

dag = DAG('test', catchup=False, default_args = default_args)

# t1 = PythonOperator()

# t2 - PythonOperator()

# t1 >> t2