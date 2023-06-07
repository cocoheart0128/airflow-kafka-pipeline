from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from miniproject_script.data_collect import data_collect
from miniproject_script.producer import producer
from miniproject_script.mysql_insert import mysql_insert


def task1():
    data_collect()

def task2():
    producer()

def task3():
    mysql_insert()

def task4():
    print("Task will run in next hour.")

# Define the DAG
dag = DAG(
    'miniproject_dag',
    description=' DAG with four steps',
    schedule='@hourly',
    start_date=datetime(2023, 6, 7),
)

# Define the PythonOperators
task1_operator = PythonOperator(
    task_id='task1_collect_data_to_s3',
    python_callable=task1,
    dag=dag,
)

task2_operator = PythonOperator(
    task_id='task2_kafka_producer',
    python_callable=task2,
    dag=dag,
)

task3_operator = PythonOperator(
    task_id='task3_kafka_consumer',
    python_callable=task3,
    dag=dag,
)

task4_operator = PythonOperator(
    task_id='task4_result',
    python_callable=task4,
    dag=dag,
)

# Define the task dependencies
task1_operator >> task2_operator >> task3_operator >>task4_operator
