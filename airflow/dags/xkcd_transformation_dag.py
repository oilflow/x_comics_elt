from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import sys

scripts_dir = '/opt/scripts'
if scripts_dir not in sys.path:
    sys.path.insert(0, scripts_dir)

from comics_transformations import (create_model_if_not_exist, check_last_comic_in_dwh_and_raw_save_in_temp_table,
                                    fill_dim_comic_author, fill_dim_date, fill_dim_comics, fill_dim_comics,
                                    fill_fact_comic_metrics)
from db_utils import delete_temp_table


default_args = {
    'owner': 'airflow',
    'start_date': '2025-04-11',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'xkcd_comics_transformation_dag',
    default_args=default_args,
    schedule_interval='0 0 * * 2,4,6',
    catchup=False,
)

create_model_if_not_exist = PythonOperator(
    task_id='create_model_if_not_exist',
    python_callable=create_model_if_not_exist,
    dag=dag
)

check_last_comic_in_dwh_and_raw_save_in_temp_table = PythonOperator(
    task_id='check_last_comics_in_dwh_and_raw_save_in_temp_table',
    python_callable=check_last_comic_in_dwh_and_raw_save_in_temp_table,
    dag=dag
)

add_dim_comic_author = PythonOperator(
    task_id='add_dim_comic_author',
    python_callable=fill_dim_comic_author,
    dag=dag
)

add_dim_date = PythonOperator(
    task_id='add_dim_date',
    python_callable=fill_dim_date,
    dag=dag
)

add_dim_comics = PythonOperator(
    task_id='add_dim_comics',
    python_callable=fill_dim_comics,
    dag=dag
)

add_fact_comics_metrics = PythonOperator(
    task_id='add_fact_comics_metrics',
    python_callable=fill_fact_comic_metrics,
    dag=dag
)

delete_temp_table = PythonOperator(
    task_id='delete_temp_table',
    python_callable=delete_temp_table,
    dag=dag
)

create_model_if_not_exist >> check_last_comic_in_dwh_and_raw_save_in_temp_table
check_last_comic_in_dwh_and_raw_save_in_temp_table >> add_dim_comic_author
add_dim_comic_author >> add_dim_date
add_dim_date >> add_dim_comics
add_dim_comics >> add_fact_comics_metrics
add_fact_comics_metrics >> delete_temp_table
