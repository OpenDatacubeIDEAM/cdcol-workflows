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
    'classes':4
}


args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "kmeans",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='kmeans', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

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


mosaic = dag_utils.OneReduce(medians, algorithm="joiner", version="1.0", dag=dag, taxprefix="mosaic")

kmeans = dag_utils.IdentityMap(
    mosaic,
    algorithm="k-means-wf",
    version="1.0",
    taxprefix="kmeans_",
    dag=dag,
    params={'classes': _params['classes']}
)

reduce= CDColReduceOperator(
    task_id='print_context',
    algorithm='test-reduce',
    version='1.0',
    dag=dag)

map(lambda b: b >> reduce, kmeans)