# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (0,2),
    'lon': (-74,-72),
    'time_ranges': [("2014-01-01", "2014-12-31")],
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
}

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "compuestoDeMedianas",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='compuesto_de_medianas', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15))

maskedLS8 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'],
                                     time_ranges=_params['time_ranges'],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS8_OLI_LASRC",
                                     params={
                                         'normalized':_params['normalized'],
                                         'bands': _params['bands'],
                                         'minValid': _params['minValid'],
                                     },
                                     dag=dag, taxprefix="maskedLS8_")

medians = dag_utils.IdentityMap(
    maskedLS8,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

mosaic = CDColReduceOperator(
    task_id='print_context',
    algorithm='joiner',
    version='1.0',
    dag=dag)

map(lambda b: b >> mosaic, medians)