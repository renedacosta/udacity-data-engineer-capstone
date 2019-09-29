from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import PostgresOperator

default_args = {
    'owner': 'RDC',
    'start_date': datetime(2019, 22, 9),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup':False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('data-harvester',
          default_args=default_args,
          description='Harvest files and Load s3 bucket and log to postgres DB',
          schedule_interval='0'
        )

create_operator = PostgresOperator(
    task_id='Begin_execution',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="create_tables.sql"
)

create_operator = PostgresOperator(
    task_id='Begin_execution',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="create_staging.sql"
)

# add multiple dimension loads with data checks

load_dimension = PostgresOperator(
    task_id='Begin_execution',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="load_dimension.sql"
)

load_facts = PostgresOperator(
    task_id='Begin_execution',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="load_facts.sql"
)


