#!/usr/bin/python3
# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (4,6),
	'lon': (-71,-69),
	'time_ranges': ("2017-01-01", "2017-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'modelos':'/web_storage/downloads/models/',
    'products': ["LS8_OLI_LASRC"],
    'generate-geotiff': True
}

_queues = {

    'mascara-landsat': queue_utils.assign_queue(input_type='multi_temporal', time_range=_params['time_ranges']),
    'joiner-reduce': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'], unidades=len(_params['products'])),
    'compuesto-temporal-medianas-wf':queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'], unidades=len(_params['products']) ),
    'joiner': queue_utils.assign_queue(input_type='multi_area',lat=_params['lat'], lon=_params['lon'] ),
    'clasificador-generico-wf': queue_utils.assign_queue(input_type='multi_area',lat=_params['lat'], lon=_params['lon'] ),
    'test-reduce': queue_utils.assign_queue(input_type='multi_area',lat=_params['lat'], lon=_params['lon'] ),
}

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "clasificador_generico",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id=args["execID"], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

masked0=dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
	time_ranges= _params['time_ranges'],
	algorithm="mascara-landsat", version="1.0",
        product=_params['products'][0],
        params={'bands':_params['bands']},
        queue=_queues['mascara-landsat'], dag=dag,  task_id="masked_"+_params['products'][0]

)
if len(_params['products']) > 1:
	masked1 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
									   time_ranges=_params['time_ranges'],
									   algorithm="mascara-landsat", version="1.0",
									   product=_params['products'][1],
									   params={'bands': _params['bands']},
                                       queue=_queues['mascara-landsat'], dag=dag, task_id="masked_{}"+_params['products'][1]

									   )
	full_query = dag_utils.reduceByTile(masked0 + masked1, algorithm="joiner-reduce", version="1.0", queue=_queues['joiner-reduce'], dag=dag, task_id="joined", params={'bands': _params['bands']})
else:
	full_query = masked0


medians = dag_utils.IdentityMap(
    full_query,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    task_id="medianas",
    queue=_queues['compuesto-temporal-medianas-wf'],
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })


mosaic = CDColReduceOperator(task_id="mosaic", algorithm="joiner", version="1.0", queue=_queues['joiner'], trigger_rule=TriggerRule.NONE_FAILED, dag=dag)

generic_classification = CDColFromFileOperator(task_id="clasificador_generico",  algorithm="clasificador-generico-wf", version="1.0", queue=_queues['clasificador-generico-wf'], dag=dag,  lat=_params['lat'], lon=_params['lon'], params={
        'bands': _params['bands'],
        'modelos': _params['modelos']+args["execID"]
    })

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


workflow= [medians >> mosaic >> generic_classification]
if _params['generate-geotiff']:
    workflow = dag_utils.BashMap(workflow, task_id="generate-geotiff", algorithm="generate-geotiff", version="1.0", queue=_queues['joiner'], dag=dag)

workflow >> delete_partial_results