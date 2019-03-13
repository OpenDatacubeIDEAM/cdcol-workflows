# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator
from datetime import timedelta
from pprint import pprint


args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10_wofs_paso_2_series_de_tiempo",
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
                           params={
                               'bands':["blue", "green", "red", "nir", "swir1", "swir2"],
                           },
                           queue='airflow_small',dag=dag, task_id="wofs_clasificacion")

series_de_tiempo = CDColFromFileOperator(algorithm="wofs-time-series-wf",
                                         version="1.0",
                                         task_id="wofs_series_de_tiempo",
                                         queue='airflow_small',
                                         dag=dag)



clasificacion>>series_de_tiempo