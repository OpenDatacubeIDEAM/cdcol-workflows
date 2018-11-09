# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,11),
    'lon': (-76,-74),
    'time_ranges': [("2013-01-01", "2013-12-31"), ("2014-01-01", "2014-12-31")],
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'classes':4
}

_queues = {

    'mascara-landsat': queue_utils.assign_queue(time_range=_params['time_ranges'], entrada_multi_temporal=False, tiles=1,  ),
    'joiner-reduce': queue_utils.assign_queue(time_range=_params['time_ranges'], entrada_multi_temporal=True, tiles=1 ),
    'compuesto-temporal-medianas-wf':queue_utils.assign_queue(time_range=_params['time_ranges'], entrada_multi_temporal=True, tiles=1 ),
    'joiner': queue_utils.assign_queue(time_range=_params['time_ranges'], entrada_multi_temporal=False, tiles=1 ),
    'deteccion-cambios-pca-wf': queue_utils.assign_queue(time_range=_params['time_ranges'], entrada_multi_temporal=False, tiles=1 ),
    'test-reduce': queue_utils.assign_queue(time_range=_params['time_ranges'], entrada_multi_temporal=False, tiles=1),
}


args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "deteccionDeCambiosPCA",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='deteccion_cambios_PCA', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

period1 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'], time_ranges=_params['time_ranges'][0],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS8_OLI_LASRC",
                                     params={'bands': _params['bands']},
                                    queue=_queues['mascara-landsat'], dag=dag, taxprefix="period1_")

period2 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'], time_ranges=_params['time_ranges'][1],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS8_OLI_LASRC",
                                     params={'bands': _params['bands']},
                                   queue=_queues['mascara-landsat'], dag=dag, taxprefix="period2_")
medians1 = dag_utils.IdentityMap(
   period1,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    queue=_queues['compuesto-temporal-medianas-wf'],
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

medians2 = dag_utils.IdentityMap(
   period2,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    queue=_queues['compuesto-temporal-medianas-wf'],
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

mosaic1 = dag_utils.OneReduce(medians1, algorithm="joiner", version="1.0", queue=_queues['joiner'], dag=dag, taxprefix="mosaic1")

mosaic2 = dag_utils.OneReduce(medians2, algorithm="joiner", version="1.0", queue=_queues['joiner'], dag=dag, taxprefix="mosaic2")

pca = dag_utils.reduceByTile(mosaic1+mosaic2, algorithm="deteccion-cambios-pca-wf", version="1.0", queue=_queues['deteccion-cambios-pca-wf'], dag=dag, taxprefix="pca_")


reduce= CDColReduceOperator(
    task_id='print_context',
    algorithm='test-reduce',
    version='1.0',
    queue=_queues['test-reduce'],
    dag=dag)

map(lambda b: b >> reduce, pca)