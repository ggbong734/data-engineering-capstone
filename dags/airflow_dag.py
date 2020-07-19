from datetime import datetime, timedelta
import os
import configparser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator, StageParquetToRedshiftOperator)
from operators.stage_parquet_redshift import StageParquetToRedshiftOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

config = configparser.ConfigParser()
config.read(['/home/workspace/airflow/dags/aws.cfg', 'aws.cfg'])

AWS_IAM_ROLE_ARN = config.get('AWS_IAM_ROLE','ARN')

# AWS_IAM_ROLE_ARN = os.environ.get('AWS_IAM_ROLE') # this extracts environment variable from .bashrc usually in root folder
# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 7, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'catchup': False, 
    'email_on_retry': False
}

dag = DAG('udac_capstone',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 * * *', # @daily
          end_date=datetime(2020, 7, 13, 0, 0, 0) # end time for debugging so run dag 2 times
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Create tables in Redshift to store S3 data
create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id='redshift', #need to add redshift connection in airflow
    sql='create_tables.sql'
)

stage_immigration_to_redshift = StageParquetToRedshiftOperator(
    task_id='Stage_immigration',
    dag=dag,
    table='staging_immigration',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='dend-bucket-oregon-123',
    s3_key='capstone_immigration/immigration_parquet',   # s3 does not support wildcard such as *
    iam_role_arn = AWS_IAM_ROLE_ARN
)

stage_states_to_redshift = StageToRedshiftOperator(
    task_id='Stage_states',
    dag=dag,
    table='states',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='dend-bucket-oregon-123',
    s3_key='capstone_immigration/states'
)

stage_airport_code_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airport_code',
    dag=dag,
    table='airport_code',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='dend-bucket-oregon-123',
    s3_key='capstone_immigration/airport_code'
)

stage_countries_to_redshift = StageToRedshiftOperator(
    task_id='Stage_countries',
    dag=dag,
    table='countries',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='dend-bucket-oregon-123',
    s3_key='capstone_immigration/countries'
)

stage_trans_mode_to_redshift = StageToRedshiftOperator(
    task_id='Stage_trans_mode',
    dag=dag,
    table='staging_trans_mode',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='dend-bucket-oregon-123',
    s3_key='capstone_immigration/trans_mode'
)

stage_visa_code_to_redshift = StageToRedshiftOperator(
    task_id='Stage_visa_code',
    dag=dag,
    table='staging_visa_code',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='dend-bucket-oregon-123',
    s3_key='capstone_immigration/visa_code'
)

load_arrivals_table = LoadFactOperator(
    task_id='Load_arrivals_fact_table',
    dag=dag,
    conn_id='redshift',
    sql=SqlQueries.arrivals_table_insert,
    target_table='arrivals'
)

load_admissions_table = LoadFactOperator(
    task_id='Load_admissions_dim_table',
    dag=dag,
    conn_id='redshift',
    sql=SqlQueries.admissions_table_insert,
    target_table='admissions'
)

load_time_table = LoadFactOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id='redshift',
    sql=SqlQueries.time_table_insert,
    target_table='time'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    dq_checks=[
         {'check_sql': "SELECT COUNT(*) FROM arrivals WHERE cicid IS NULL", 'expected_result':0},   
         {'check_sql': "SELECT COUNT(*) FROM admissions WHERE admnum IS NULL", 'expected_result':0},
         {'check_sql': "SELECT COUNT(*) FROM time WHERE arrdate IS NULL", 'expected_result':0},
         {'check_sql': "SELECT COUNT(*) FROM states WHERE state_code IS NULL", 'expected_result':0},
         {'check_sql': "SELECT COUNT(*) FROM states WHERE state_code IS NULL", 'expected_result':0}
        ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables

create_tables >> [stage_immigration_to_redshift , stage_states_to_redshift, stage_airport_code_to_redshift, stage_countries_to_redshift, stage_trans_mode_to_redshift, stage_visa_code_to_redshift]

stage_immigration_to_redshift >> [load_arrivals_table, load_admissions_table, load_time_table]
stage_states_to_redshift >> load_arrivals_table
stage_airport_code_to_redshift >> run_quality_checks
stage_countries_to_redshift >> run_quality_checks
stage_trans_mode_to_redshift >> load_arrivals_table 
stage_visa_code_to_redshift >> load_admissions_table

load_arrivals_table >> run_quality_checks
load_admissions_table >> run_quality_checks
load_time_table >> run_quality_checks

run_quality_checks >> end_operator