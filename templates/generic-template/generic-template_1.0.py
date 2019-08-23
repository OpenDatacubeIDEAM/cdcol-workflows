import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule
from cdcol_plugin.operators import common
from datetime import timedelta
from pprint import pprint

_params = {{params}}

_params['bands']=_params['products'][0]['bands']

_steps = {
    'generic-step': {
        'algorithm': _params['algorithm_name'],
        'version': _params['algorithm_version'],
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal_unidad',
            time_range=_params['time_ranges'][0],
            unidades=len(_params['products'])
        ),
        'params': _params,
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
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id=args['execID'],
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120)
)

generic_step = dag_utils.queryMapByTile(
    lat=_params['lat'],
    lon=_params['lon'],
    time_ranges=_params['time_ranges'],
    algorithm=_steps['generic-step']['algorithm'],
    version=_steps['generic-step']['version'],
    product=_params['products'][0],
    params=_steps['generic-step']['params'],
    queue=_steps['generic-step']['queue'],
    dag=dag,
    task_id="generic-step_" + _params['products'][0]['name'], 
    to_tiff=False, 
    alg_folder=common.COMPLETE_ALGORITHMS_FOLDER
)

workflow = generic_step
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
        to_tiff=False
    )

    workflow = mosaico

workflow

