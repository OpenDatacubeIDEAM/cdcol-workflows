#!/usr/bin/python3
# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,11),
    'lon': (-76,-75),
    'time_ranges': [("2014-01-01", "2014-12-31"), ("2015-01-01", "2015-12-31")],
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':False,
    'products': ["LS7_ETM_LEDAPS"],
    'classes':4
}

_queues = {

    'mascara-landsat': queue_utils.assign_queue(input_type='multi_temporal', time_range=_params['time_ranges'][0]),
    'joiner-reduce': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'][0], unidades=len(_params['products'])),
    'compuesto-temporal-medianas-wf':queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'][0], unidades=len(_params['products']) ),
    'joiner': queue_utils.assign_queue(input_type='multi_area',lat=_params['lat'], lon=_params['lon'] ),
    'deteccion-cambios-pca-wf': queue_utils.assign_queue(input_type='multi_area',lat=_params['lat'], lon=_params['lon'] ),
    'test-reduce': queue_utils.assign_queue(input_type='multi_area',lat=_params['lat'], lon=_params['lon'] ),
}




args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "deteccion_de_cambios_PCA",
    'product': _params['products'][0]
}

dag = DAG(
    dag_id=args["execID"], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

period1 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'], time_ranges=_params['time_ranges'][0],
                                     algorithm="mascara-landsat", version="1.0",
                                     product=_params['products'][0],
                                     params={'bands': _params['bands']},
                                    queue=_queues['mascara-landsat'], dag=dag, taxprefix="period1_")

period2 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'], time_ranges=_params['time_ranges'][1],
                                     algorithm="mascara-landsat", version="1.0",
                                     product=_params['products'][0],
                                     params={'bands': _params['bands']},
                                   queue=_queues['mascara-landsat'], dag=dag, taxprefix="period2_")
medians1 = dag_utils.IdentityMap(
   period1,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_1",
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
    taxprefix="medianas_2",
    queue=_queues['compuesto-temporal-medianas-wf'],
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })
print(queue_utils.get_tiles(_params['lat'],_params['lon']))
if queue_utils.get_tiles(_params['lat'],_params['lon'])>1:
    mosaic1 = dag_utils.OneReduce(medians1, algorithm="joiner", version="1.0", queue=_queues['joiner'], dag=dag, taxprefix="mosaic_1")
    mosaic2 = dag_utils.OneReduce(medians2, algorithm="joiner", version="1.0", queue=_queues['joiner'], dag=dag, taxprefix="mosaic_2")
    results = mosaic1+mosaic2
else:
    results = medians1+medians2


pca = dag_utils.reduceByTile(results, algorithm="deteccion-cambios-pca-wf", version="1.0", queue=_queues['deteccion-cambios-pca-wf'], dag=dag, params={'bands':_params['bands']}, taxprefix="pca")


delete_partial_results = PythonOperator(task_id='delete_partial_results',
                                        provide_context=True,
                                        python_callable=other_utils.delete_partial_results,
                                        queue='airflow_small',
                                        op_kwargs={'algorithms':{
                                            'mascara-landsat': "1.0",
                                            'joiner-reduce': "1.0",
                                            'compuesto-temporal-medianas-wf':"1.0",
                                        }, 'execID': args['execID']},
                                        dag=dag)
pca >> delete_partial_results