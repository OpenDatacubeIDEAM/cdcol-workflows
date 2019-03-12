# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator
from datetime import timedelta
from pprint import pprint


args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10_paso_2_series_de_tiempo",
    'product':"LS8_OLI_LASRC"
}

dag = DAG(
    dag_id=args['execID'], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

clasificacion = CDColQueryOperator(algorithm="wofs-wf",
                           version="1.0",
                           lat=(10,11),
                           lon=(-75,-74),
                           product="LS8_OLI_LASRC",
                           time_ranges=("2015-01-01", "2015-12-31"),
                           params={
                               'bands':["blue", "green", "red", "nir", "swir1", "swir2"],
                           },
                           queue='airflow_small',dag=dag, task_id="wofs_")

series_de_tiempo = CDColFromFileOperator(algorithm="wofs-time-series-wf",
                                         version="1.0",
                                         task_id="wofs_time_series_",
                                         queue='airflow_small',
                                         dag=dag)



clasificacion>>series_de_tiempo