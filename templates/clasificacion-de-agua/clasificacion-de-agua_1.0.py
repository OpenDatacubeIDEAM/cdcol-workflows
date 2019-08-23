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
    'wofs': {
        'algorithm': "wofs-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(),
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
    'serie_tiempo': {
        'algorithm': "wofs-time-series-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal_unidad',
            time_range=_params['time_ranges'][0],
            unidades=len(_params['products'])
        ),
        'params': {},
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
    dag_id=args["execID"], 
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120)
)

wofs = dag_utils.queryMapByTileByYear(
    lat=_params['lat'], 
    lon=_params['lon'],
    time_ranges=_params['time_ranges'][0], 
    product=_params['products'][0],
    algorithm=_steps['wofs']['algorithm'], 
    version=_steps['wofs']['version'],
    queue=_steps['wofs']['queue'],
    dag=dag, 
    task_id="wofs"
)

reduccion = dag_utils.reduceByTile(
    wofs, 
    algorithm=_steps['reduccion']['algorithm'],
    version=_steps['reduccion']['version'],
    queue=_steps['reduccion']['queue'], 
    dag=dag, 
    task_id="joined",
    delete_partial_results=_steps['reduccion']['del_prev_result'],
    params=_steps['reduccion']['params'], 
)

serie_tiempo=dag_utils.IdentityMap(
    reduccion, 
    algorithm=_steps['serie_tiempo']['algorithm'],
    version=_steps['serie_tiempo']['version'], 
    task_id="wofs_serie_tiempo",
    queue=_steps['serie_tiempo']['queue'], 
    delete_partial_results=_steps['serie_tiempo']['del_prev_result'],
    dag=dag,  
    to_tiff= not (_params['genera_mosaico'] and queue_utils.get_tiles(_params['lat'],_params['lon'])>1)
)

workflow = serie_tiempo
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
