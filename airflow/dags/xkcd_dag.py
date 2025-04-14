from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import sys

scripts_dir = '/opt/scripts'
if scripts_dir not in sys.path:
    sys.path.insert(0, scripts_dir)

from db_utils import is_raw_table_exists
from comics_ingestion_and_preparing import (get_all_comics_data, check_latest_comic_in_db_and_api, get_missing_comics,
                                            branch_comic_import_based_on_ids, import_new_comic, wait_for_new_data)


default_args = {
    'owner': 'airflow',
    'start_date': '2025-04-11',
}

dag = DAG(
    'xkcd_comics_dag',
    default_args=default_args,
    schedule_interval='0 0 * * 1,3,5',
    catchup=False,
)

check_raw_table = BranchPythonOperator(
    task_id='check_raw_table',
    python_callable=is_raw_table_exists,
    retries=3,
    retry_delay=timedelta(minutes=2),
    dag=dag
)

import_all_comics = PythonOperator(
    task_id='import_all_comics',
    python_callable=get_all_comics_data,
    retries=3,
    retry_delay=timedelta(minutes=2),
    dag=dag
)

check_latest_comic = PythonOperator(
    task_id='check_latest_comic',
    python_callable=check_latest_comic_in_db_and_api,
    provide_context=True,
    retries=3,
    retry_delay=timedelta(minutes=2),
    dag=dag
    )

comic_import_based_on_ids = BranchPythonOperator(
    task_id='comic_import_based_on_ids',
    python_callable=branch_comic_import_based_on_ids,
    provide_context=True,
    dag=dag
)

import_missing_comics = PythonOperator(
    task_id='import_missing_comics',
    python_callable=get_missing_comics,
    provide_context=True,
    retries=3,
    retry_delay=timedelta(minutes=2),
    dag=dag
    )

import_new_comic = PythonOperator(
    task_id='import_new_comic',
    python_callable=import_new_comic,
    retries=3,
    retry_delay=timedelta(minutes=2),
    dag=dag
    )

wait_for_new_comic = PythonOperator(
    task_id='wait_for_new_comic',
    python_callable=wait_for_new_data,
    retries=46,
    retry_delay=timedelta(minutes=30),
    dag=dag
)

check_raw_table >> [import_all_comics, check_latest_comic]
check_latest_comic >> comic_import_based_on_ids
comic_import_based_on_ids >> [import_missing_comics, import_new_comic, wait_for_new_comic]


