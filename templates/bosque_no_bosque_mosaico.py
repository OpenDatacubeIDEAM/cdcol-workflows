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
        'queue': queue_utils.assign_queue(input_type='multi_area', lat=_params['lat'], lon=_params['lon']),
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
    dag_id=args["execID"], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20)
)


ndvi = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                product=_params['products'][0],
                                time_ranges=_params['time_ranges'][0],
                                algorithm=_steps['ndvi']['algorithm'],
                                version=_steps['ndvi']['version'],
                                params=_steps['ndvi']['params'],
                                queue=_steps['ndvi']['queue'],
                                delete_partial_results=_steps['ndvi']['del_prev_result'], dag=dag,
                                task_id="ndvi", to_tiff= not (_params['genera_mosaico'] and queue_utils.get_tiles(_params['lat'],_params['lon'])>1))


bosque = dag_utils.IdentityMap(ndvi, algorithm=_steps['bosque']['algorithm'],
                               product=_params['products'][0],
                               version=_steps['bosque']['version'], params=_steps['bosque']['params'],
                               queue=_steps['bosque']['queue'], delete_partial_results=_steps['ndvi']['del_prev_result'], dag=dag,
                               task_id="bosque", to_tiff= not( _params['genera_mosaico'] and queue_utils.get_tiles(_params['lat'],_params['lon'])>1))

workflow = bosque
if _params['genera_mosaico'] and queue_utils.get_tiles(_params['lat'],_params['lon'])>1:
    mosaico = dag_utils.OneReduce(workflow, task_id="mosaic", algorithm=_steps['mosaico']['algorithm'],
                                  version=_steps['mosaico']['version'], queue=_steps['mosaico']['queue'],
                                  delete_partial_results=_steps['mosaico']['del_prev_result'],
                                  trigger_rule=TriggerRule.NONE_FAILED, dag=dag, to_tiff=True)

    workflow = mosaico

workflow
