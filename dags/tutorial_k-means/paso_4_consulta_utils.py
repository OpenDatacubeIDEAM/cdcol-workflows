# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils, queue_utils

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (0,2),
    'lon': (-70, -69),
    'time_ranges': ("2017-01-01", "2017-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'classes':4,
    'products': ["LS8_OLI_LASRC"],
}

args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10-paso-4-consulta-utils",
    'product':_params['products'][0]
}

dag = DAG(
    dag_id='mp.mancipe10-paso-4-consulta-utils', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

consulta=dag_utils.queryMapByTile(lat=_params['lat'],
                                 lon=_params['lon'],
                                 time_ranges= _params['time_ranges'],
                                  algorithm="mascara-landsat",
                                  version="1.0",
                                  product=_params['products'][0],
                                  params={'bands':_params['bands']},
                                  queue='airflow_small',
                                  dag=dag, taxprefix="masked_{}_".format(_params['products'][0]))

consulta