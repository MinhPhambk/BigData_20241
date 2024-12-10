from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperators

default_args = {
    'owner': 'ngocminh',
    'depends_on_past': False,
    'email': ['phamngocminh1230@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

my_dag = DAG('Stream_from_Kafka_to_Spark',
    default_args=default_args,
    start_date=datetime(2023, 11, 3),
    schedule_interval='@once',
    description='Stream spark to database',
    catchup=False)

spark_to_db_task = SparkSubmitOperator(
    task_id='stream_spark',
    conn_id='spark_conn',
    application='jobs/spark_stream_from_kafka.py',
    packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1',
    dag=my_dag
)