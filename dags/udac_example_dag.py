from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Tarek Atwan',
    'start_date': datetime(2021, 2, 21, 0,0,0,0),
    'depends_on_past': False,
    'email': ['tatwan@outlook.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json="auto ignorecase"
)

load_songplays_table = LoadFactOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    task_id='Load_songplays_fact_table',
    dag=dag,
    fields="(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)",
    table="public.songplays",
    query=SqlQueries.songplay_table_insert,
    append_data=False
)

load_user_dimension_table = LoadDimensionOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    task_id='Load_user_dim_table',
    dag=dag,
    table="public.users",
    fields="(userid, first_name, last_name, gender, level)",
    query=SqlQueries.user_table_insert,
    append_data=False

)

load_song_dimension_table = LoadDimensionOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    task_id='Load_song_dim_table',
    dag=dag,
    table="public.songs",
    fields="(songid, title, artistid, year, duration)",
    query=SqlQueries.song_table_insert,
    append_data=False
)

load_artist_dimension_table = LoadDimensionOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    task_id='Load_artist_dim_table',
    dag=dag,
    table="public.artists",
    fields="(artistid, name, location, lattitude, longitude)",
    query=SqlQueries.artist_table_insert,
    append_data=False
)

load_time_dimension_table = LoadDimensionOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    task_id='Load_time_dim_table',
    dag=dag,
    table="public.time",
    fields="(start_time, hour, day, week, month, year, weekday)",
    query=SqlQueries.time_table_insert,
    append_data=False
)

run_quality_checks = DataQualityOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_check=[
              {'check_sql':'SELECT count(*) FROM public.users WHERE userid is null', 'expected_result':0},
              {'check_sql':'SELECT count(*) FROM public.artists WHERE artistid is null', 'expected_result':0},
              {'check_sql':'SELECT count(*) FROM public.songs WHERE songid is null', 'expected_result':0},
              {'check_sql':'SELECT count(*) FROM public."time" WHERE start_time is null', 'expected_result':0}
            ]              
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
run_quality_checks >> end_operator