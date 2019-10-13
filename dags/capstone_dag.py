from datetime import datetime, timedelta
import os
import glob
from pathlib import Path
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
    op_kwargs={'file':'./data/GlobalLandTemperaturesByCity.csv', 'table':'staging.city_temp', 'hook':'postgres'}
)
stage_i94port = PythonOperator(
    task_id='stage_i94port',  
    dag=dag,
    python_callable=load_staging_csv,
    op_kwargs={'file':'./data/i94port.csv', 'table':'staging.i94port', 'hook':'postgres'}
)
stage_i94res = PythonOperator(
    task_id='stage_i94port',  
    dag=dag,
    python_callable=load_staging_sas,
    op_kwargs={'file':'./data/i94res.csv', 'table':'staging.i94res', 'hook':'postgres'}
)
# 
stage_immi = []
for f in glob.glob('../data/*sas7bdat'):
    name = Path(f).name
    stage_immi.append( 
        PythonOperator(
            task_id=f'stage_immi_{name}',
            dag=dag,
            python_callable=load_staging_sas,
            op_kwargs={'file':'./data/immigration_data_sample.csv', 'hook':'postgres'}
        )
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

# load fact table
load_immi = PostgresOperator(
    task_id='load_immi',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="load_immigration.sql"
)

# load fact table
load_sum = PostgresOperator(
    task_id='load_summary',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="load_summary.sql"
)

# data quality checks
data_quality = PostgresOperator(
    task_id='data_quality',  
    dag=dag,
    postgres_conn_id="postgres",
    sql="data_quality.sql"
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
[stage_i94res, stage_temperature] >> load_cities

stage_immi >> load_immi
load_cities, load_countries >> load_immi

load_immi >> load_sum >> data_quality >> cleanup
