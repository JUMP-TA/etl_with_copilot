
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os
import sys

# Add src directory to the system path
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../src'))

# Import ETL functions
from etl import extract_data, transform_data, load_data

def etl_pipeline():
    print("Setting up pipeline...")
    # Set up the Spark session
    spark = SparkSession.builder.appName("Airflow ETL").getOrCreate()

    # Define file paths
    input_file = "data/input.csv"
    output_file = "data/output"

    # Execute ETL process
    data = extract_data(spark, input_file)
    transformed_data = transform_data(data)
    load_data(transformed_data, output_file)

    # Stop the Spark session
    spark.stop()
    print("Finito!")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'simple_etl_dag_from_script',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
) as dag:

    # Define the ETL task
    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=etl_pipeline,
    )

    etl_task
