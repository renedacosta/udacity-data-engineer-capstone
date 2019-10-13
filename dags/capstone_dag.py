from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators import PostgresOperator
from helpers.stage_file import load_staging_csv, load_staging_sas

default_args = {
    'owner': 'RDC',
    'start_date': datetime(2019, 22, 9),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'catchup':False,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Capstone',
          default_args=default_args,
          description='Harvest files and Load s3 bucket and log to postgres DB',
          schedule_interval='0'
        )

create_analytics = PostgresOperator(
    task_id='create_analytics',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="create_analytics.sql"
)

create_staging = PostgresOperator(
    task_id='create_staging',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="create_staging.sql"
)

# load staging tables
stage_temperature = PythonOperator(
    task_id='stage_temperature',  
    dag=dag,
    python_callable=load_staging_csv,
    op_kwargs={'file':'./data/GlobalLandTemperaturesByCity.csv', 'table':'staging.city_temp'}
)
stage_i94port = PythonOperator(
    task_id='stage_i94port',  
    dag=dag,
    python_callable=load_staging_csv,
    op_kwargs={'file':'./data/i94port.csv', 'table':'staging.i94port'}
)
stage_i94res = PythonOperator(
    task_id='stage_i94port',  
    dag=dag,
    python_callable=load_staging_sas,
    op_kwargs={'file':'./data/i94res.csv', 'table':'staging.i94res'}
)
# add for loop here
stage_immi = PythonOperator(
    task_id='stage_immi',  
    dag=dag,
    python_callable=load_staging_sas,
    op_kwargs={'file':'./data/immigration_data_sample.csv'}
)

# load dimension loads with data checks
load_countries = PostgresOperator(
    task_id='load_countries',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="load_countries.sql"
)
load_cities = PostgresOperator(
    task_id='load_cities',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="load_cities.sql"
)
load_temperature = PostgresOperator(
    task_id='load_temperature',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="load_temperature.sql"
)


# load fact table
load_immi = PostgresOperator(
    task_id='load_immi',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="load_immigration.sql"
)

# data quality checks
cleanup = PostgresOperator(
    task_id='cleanup',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="DROP SCHEMA IF EXISTS staging CASCADE"
)

# cleanup (remove staging)
cleanup = PostgresOperator(
    task_id='cleanup',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="DROP SCHEMA IF EXISTS staging CASCADE"
)


# Pipeline


create_staging >> stage_i94port
create_staging >> stage_i94res
create_staging >> stage_temperature
create_staging >> stage_immi

stage_i94port >> load_countries
stage_i94res, stage_temperature >> load_cities

load_cities >> load_temperature

load_temperature, load_cities, load_countries >> load_immi

load_immi >> data_quality

data_quality >> cleanup
