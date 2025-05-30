from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import sys
sys.path.append('/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/retail_sales_pipeline/')
from main import main


with DAG(  'retail_sales_pipeline',
            start_date=datetime(2025,5,28),
            schedule = '@daily',
            catchup=False) as dag:
    t1 = BashOperator(task_id = 'run_main_script', bash_command='/home/manish/Documents/Spark-Tutorial/DE_Complete_Project_1/retail_sales_pipeline/main.py')