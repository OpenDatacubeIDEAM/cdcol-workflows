import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {{params}}


args = {
    'owner':  _params['owner'],
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': _params['execID'],
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id=args["execID"], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

_steps = {
    'consulta': {
        'algorithm': "just-query",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area',
                                          lat=_params['lat'],
                                          lon=_params['lon']),
        'params': {},
    },
    'entrenamiento': {
        'algorithm': "random-forest-training",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area',
                                          lat=_params['lat'],
                                          lon=_params['lon']),
        'params': {'train_data_path': _params['modelos'] + args["execID"]},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'clasificador': {
        'algorithm': "clasificador-generico-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area',
                                          lat=_params['lat'],
                                          lon=_params['lon']),
        'params': {'modelos': _params['modelos'] + args["execID"]},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    }

}

mosaico = CDColQueryOperator(lat=_params['lat'], lon=_params['lon'],
                                     time_ranges=_params['time_ranges'][0],
                                     algorithm=_steps['consulta']['algorithm'],
                                     version=_steps['consulta']['version'],
                                     product=_params['products'][0],
                                     params=_steps['consulta']['params'],
                                     queue=_steps['consulta']['queue'], dag=dag,
                                     task_id="consulta_" + _params['products'][0]['name'])

entrenamiento = CDColQueryOperator(
    algorithm=_steps['entrenamiento']['algorithm'],
    version=_steps['entrenamiento']['version'],
    task_id="entrenamiento",
    queue=_steps['entrenamiento']['queue'], dag=dag,
    delete_partial_results=False,
    params=_steps['entrenamiento']['params'])

clasificador = CDColReduceOperator(task_id="clasificador_generico",
                                   algorithm=_steps['clasificador']['algorithm'],
                                   version=_steps['clasificador']['version'],
                                   queue=_steps['clasificador']['queue'],
                                   dag=dag,
                                   lat=_params['lat'], lon=_params['lon'],
                                   params=_steps['clasificador']['params'], to_tiff=True)


entrenamiento>>clasificador
workflow>>clasificador
