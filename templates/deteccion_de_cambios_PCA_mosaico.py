from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {{params}}

_steps = {
    'consulta': {
        'algorithm': "just-query",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal_area', time_range=_params['time_ranges'][0], lat=_params['lat'], lon=_params['lon']),
        'params': {},
    },
    'pca': {
        'algorithm': "deteccion-cambios-pca-wf",
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
    'product': _params['products'][0]
}

dag = DAG(
    dag_id=args["execID"], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

consulta_periodo_1 = CDColQueryOperator(lat=_params['lat'], lon=_params['lon'],
                                     time_ranges=_params['time_ranges'][0],
                                     algorithm=_steps['consulta']['algorithm'], version=_steps['consulta']['version'],
                                     product=_params['products'][0],
                                     params=_steps['consulta']['params'],
                                     queue=_steps['consulta']['queue'], dag=dag,
                                     task_id="consulta_p1_" + _params['products'][0]['name'])

consulta_periodo_2 = CDColQueryOperator(lat=_params['lat'], lon=_params['lon'],
                                     time_ranges=_params['time_ranges'][1],
                                     algorithm=_steps['consulta']['algorithm'], version=_steps['consulta']['version'],
                                     product=_params['products'][0],
                                     params=_steps['consulta']['params'],
                                     queue=_steps['consulta']['queue'], dag=dag,
                                     task_id="consulta_p2_" + _params['products'][0]['name'])


pca = dag_utils.reduceByTile([consulta_periodo_1,consulta_periodo_2],
                             task_id="pca",
                             algorithm=_steps['pca']['algorithm'],
                             version=_steps['pca']['version'],
                             queue=_steps['pca']['queue'],
                             product=_params['products'][0],
                             dag=dag,
                             delete_partial_results=_steps['pca']['del_prev_result'],
                             params=_steps['pca']['params'], to_tiff=True)

pca
