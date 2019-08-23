#coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {{params}}

_steps = {
    'mascara': {
        'algorithm': "mascara-landsat",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal',
            time_range=_params['time_ranges'][0]
        ),
        'params': {},
    },
    'medianas': {
        'algorithm': "compuesto-temporal-medianas-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal_unidad',
            time_range=_params['time_ranges'][0],
            unidades=len(_params['products'])
        ),
        'params': {'minValid': _params['minValid']},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'greenest_pixel':{
        'algorithm': "greenest_pixel-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal_unidad',
            time_range=_params['time_ranges'][0],
            unidades=len(_params['products'])
        ),
        'params': {
            'normalized': _params['normalized'],
            'minValid': _params['minValid'],
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'mosaico': {
        'algorithm': "joiner",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_area',
            lat=_params['lat'],
            lon=_params['lon']
        ),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
}

args = {
    'owner': _params['owner'],
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':_params['execID'],
    'product': _params['products'][0]['name']
}

dag = DAG(
    dag_id=args["execID"],
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120)
)

mascara = dag_utils.queryMapByTile(
    lat=_params['lat'],
    lon=_params['lon'],
    time_ranges=_params['time_ranges'][0],
    algorithm=_steps['mascara']['algorithm'],
    version=_steps['mascara']['version'],
    product=_params['products'][0],
    params=_steps['mascara']['params'],
    queue=_steps['mascara']['queue'],
    dag=dag,
    task_id="mascara" + _params['products'][0]['name']
)

greenest_pixel = dag_utils.IdentityMap(
    mascara,
    algorithm=_steps['greenest_pixel']['algorithm'],
    product=_params['products'][0],
    version=_steps['greenest_pixel']['version'],
    task_id="greenest_pixel",
    queue=_steps['greenest_pixel']['queue'],
    dag=dag,
    delete_partial_results=_steps['greenest_pixel']['del_prev_result'], 
    to_tiff=not (_params['genera_mosaico'] and  queue_utils.get_tiles(_params['lat'], _params['lon']) > 1)
)

if _params['genera_mosaico'] and  queue_utils.get_tiles(_params['lat'], _params['lon']) > 1:
    mosaico = dag_utils.OneReduce(
        greenest_pixel,
        task_id="mosaico",
        algorithm=_steps['mosaico']['algorithm'],
        version=_steps['mosaico']['version'],
        queue=_steps['mosaico']['queue'],
        delete_partial_results=_steps['mosaico']['del_prev_result'],
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag,
        to_tiff=True
    )

    mosaico
else:
    greenest_pixel

