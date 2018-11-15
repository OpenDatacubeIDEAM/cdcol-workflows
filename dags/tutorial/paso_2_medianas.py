# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,10),
    'lon': (-76,-75),
    'time_ranges': ("2013-01-01", "2015-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'products': ["LS8_OLI_LASRC"],
    'normalized':True,
    'mosaic':True,
}

args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10-paso-2-medianas",
    'product':_params['products'][0]
}

dag = DAG(
    dag_id='mp.mancipe10-paso-2-medianas', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

consulta = CDColQueryOperator(algorithm="mascara-landsat",
                           version="1.0",
                           lat=_params['lat'],
                           lon=_params['lon'],
                           product=_params['products'][0],
                           time_ranges=_params['time_ranges'],
                           params={
                               'bands':_params['bands'],
                           },
                           queue='airflow_small',dag=dag, task_id="query_")

medianas = CDColFromFileOperator(algorithm="compuesto-temporal-medianas-wf",
                           version="1.0",
                           lat=_params['lat'],
                           lon=_params['lon'],
                           product=_params['products'][0],
                           time_ranges=_params['time_ranges'],
                           params={
                               'normalized': _params['normalized'],
                               'bands': _params['bands'],
                               'minValid': _params['minValid'],
                           },
                           queue='airflow_small',dag=dag, task_id="medianas_")

consulta>>medianas