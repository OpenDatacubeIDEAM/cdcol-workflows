# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator
from datetime import timedelta
from pprint import pprint


args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10_wofs_paso_1_clasificacion_wofs",
    'product':"LS7_ETM_LEDAPS"
}

dag = DAG(
    dag_id=args['execID'], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

clasificacion = CDColQueryOperator(algorithm="wofs-wf",
                           version="1.0",
                           lat=(10,11),
                           lon=(-75,-74),
                           product="LS7_ETM_LEDAPS",
                           time_ranges=("2013-01-01", "2013-12-31"),
                           queue='airflow_small',dag=dag, task_id="wofs_clasificacion")

clasificacion