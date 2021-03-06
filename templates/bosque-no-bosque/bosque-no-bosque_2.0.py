import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {{params}}

_params['elimina_resultados_anteriores']=False

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
    'reduccion': {
        #'algorithm': "joiner-reduce",
        'algorithm': "joiner",
        'version': '1.0',
        'queue': 'airflow_xlarge',
        # 'queue': queue_utils.assign_queue(
        #     input_type='multi_temporal_unidad', 
        #     time_range=_params['time_ranges'][0],
        #     unidades=len(_params['products'])
        # ),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'medianas': {
        'algorithm': "compuesto-temporal-medianas-wf",
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
    'ndvi': {
        'algorithm': "ndvi-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'bosque': {
        'algorithm': "bosque-no-bosque-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(),
        'params': {
            'ndvi_threshold': _params['ndvi_threshold'],
            'vegetation_rate': _params['vegetation_rate'],
            'slice_size': _params['slice_size']
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
    }
}

args = {
    'owner': _params['owner'],
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': _params['execID'],
    'product':_params['products'][0]
}

dag = DAG(
    dag_id=args["execID"], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20)
)


mascara_0 = dag_utils.queryMapByTile(
    lat=_params['lat'], lon=_params['lon'],
    time_ranges=_params['time_ranges'][0],
    algorithm=_steps['mascara']['algorithm'], 
    version=_steps['mascara']['version'],
    product=_params['products'][0],
    params=_steps['mascara']['params'],
    queue=_steps['mascara']['queue'], 
    dag=dag,
    task_id="mascara_" + _params['products'][0]['name']
)

if len(_params['products']) > 1:
    mascara_1 = dag_utils.queryMapByTile(
        lat=_params['lat'], lon=_params['lon'],
        time_ranges=_params['time_ranges'][0],
        algorithm=_steps['mascara']['algorithm'],
        version=_steps['mascara']['version'],
        product=_params['products'][1],
        params=_steps['mascara']['params'],
        queue=_steps['mascara']['queue'], 
        dag=dag,
        task_id="mascara_" + _params['products'][1]['name']
    )

    reduccion = dag_utils.reduceByTile(
        mascara_0 + mascara_1, 
        algorithm=_steps['reduccion']['algorithm'],
        product=_params['products'][0],
        version=_steps['reduccion']['version'],
        queue=_steps['reduccion']['queue'], 
        dag=dag, 
        task_id="joined",
        delete_partial_results=_steps['reduccion']['del_prev_result'],
        params=_steps['reduccion']['params'], 
    )

else:
    reduccion = mascara_0

medianas = dag_utils.IdentityMap(
    reduccion,
    product=_params['products'][0],
    algorithm=_steps['medianas']['algorithm'],
    version=_steps['medianas']['version'],
    task_id="medianas",
    queue=_steps['medianas']['queue'], 
    dag=dag,
    delete_partial_results=_steps['medianas']['del_prev_result'],
    params=_steps['medianas']['params'])

ndvi = dag_utils.IdentityMap(
    medianas, 
    product=_params['products'][0],
    algorithm=_steps['ndvi']['algorithm'], 
    version=_steps['ndvi']['version'],
    queue=_steps['ndvi']['queue'], 
    delete_partial_results=_steps['ndvi']['del_prev_result'],
    dag=dag, task_id="ndvi"
)

bosque = dag_utils.IdentityMap(
    ndvi, 
    algorithm=_steps['bosque']['algorithm'],
    product=_params['products'][0],
    version=_steps['bosque']['version'], 
    params=_steps['bosque']['params'],
    queue=_steps['bosque']['queue'], 
    delete_partial_results=_steps['ndvi']['del_prev_result'], 
    dag=dag,
    task_id="bosque", 
    to_tiff= not( _params['genera_mosaico'] and queue_utils.get_tiles(_params['lat'],_params['lon'])>1)
)

workflow = bosque
if _params['genera_mosaico'] and queue_utils.get_tiles(_params['lat'],_params['lon'])>1:
    mosaico = dag_utils.OneReduce(
        workflow, 
        task_id="mosaic", 
        algorithm=_steps['mosaico']['algorithm'],
        version=_steps['mosaico']['version'], 
        queue=_steps['mosaico']['queue'],
        delete_partial_results=_steps['mosaico']['del_prev_result'],
        trigger_rule=TriggerRule.NONE_FAILED, 
        dag=dag, 
        to_tiff=True
    )

    workflow = mosaico

workflow

