from airflow.operators import CompressFileSensor
from cdcol_utils import other_utils
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params={{params}}
_params['elimina_resultados_anteriores']=True

args = {
    'owner':  _params['owner'],
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': _params['execID'],
    'product': _params['products'][0]
}

dag = DAG(
    dag_id=args["execID"],
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120)
)


_steps = {
    'mascara': {
        'algorithm': "mascara-landsat",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal',
            time_range=_params['time_ranges'][0]
        ),
        'params': {'bands': _params['products'][0]['bands']}
    },
    'reduccion': {
        #'algorithm': "joiner-reduce",
        'algorithm': "joiner",
        'version': '1.0',
        'queue': 'airflow_medium',
        # 'queue': queue_utils.assign_queue(
        #     input_type='multi_temporal_unidad',
        #     time_range=_params['time_ranges'][0],
        #     unidades=len(_params['products'])
        # ),
        'params': {'bands': _params['products'][0]['bands']},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'medianas': {
        'algorithm': "compuesto-temporal-medianas-indices-wf",
        'version': '1.0',
        'queue': 'airflow_medium',
        #'queue': queue_utils.assign_queue(
        #    input_type='multi_temporal_unidad',
        #    time_range=_params['time_ranges'][0],
        #    unidades=len(_params['products'])
        #),
        'params': {
            'minValid': _params['minValid'],
            'normalized':_params['normalized']
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'mosaico': {
        'algorithm': "joiner",
        'version': '1.0',
        'queue': 'airflow_medium',
            #queue_utils.assign_queue(
            #input_type='multi_area',
            #lat=_params['lat'],
            #lon=_params['lon']
        #),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'entrenamiento': {
        'algorithm': "ensemble-training",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_area',
            lat=_params['lat'],
            lon=_params['lon']
        ),
        'params': {
            'bands': _params['products'][0]['bands'],
            'train_data_path': _params['modelos']
        },
        #'del_prev_result': _params['elimina_resultados_anteriores'],
        #'del_prev_result': False
    },
    'clasificador': {
        'algorithm': "clasificador-ensemble-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_area',
            lat=_params['lat'],
            lon=_params['lon']
        ),
        'params': {
            'bands': _params['products'][0]['bands'],
            'modelos': _params['modelos']
        },
        #'del_prev_result': _params['elimina_resultados_anteriores'],
    }

}

mascara_0 = dag_utils.queryMapByTile(
    lat=_params['lat'], 
    lon=_params['lon'],
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
        lat=_params['lat'],
        lon=_params['lon'],
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
        product=_params['products'][0],
        algorithm=_steps['reduccion']['algorithm'],
        version=_steps['reduccion']['version'],
        queue=_steps['reduccion']['queue'],
        dag=dag, task_id="joined",
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
    params=_steps['medianas']['params']
)

workflow=medianas

if queue_utils.get_tiles(_params['lat'],_params['lon'])>1:
    mosaico = dag_utils.OneReduce(
        workflow,
        task_id="mosaic",
        algorithm=_steps['mosaico']['algorithm'],
        version=_steps['mosaico']['version'],
        queue=_steps['mosaico']['queue'],
        delete_partial_results=_steps['mosaico']['del_prev_result'],
        trigger_rule=TriggerRule.NONE_FAILED,
        dag=dag
    )

    workflow=mosaico

entrenamiento = dag_utils.IdentityMap(
    workflow,
    algorithm=_steps['entrenamiento']['algorithm'],
    version=_steps['entrenamiento']['version'],
    task_id="entrenamiento",
    queue=_steps['entrenamiento']['queue'],
    dag=dag,
    delete_partial_results=_steps['entrenamiento']['del_prev_result'],
    params=_steps['entrenamiento']['params']
)

clasificador = CDColReduceOperator(
    task_id="clasificador_generico",
    algorithm=_steps['clasificador']['algorithm'],
    version=_steps['clasificador']['version'],
    queue=_steps['clasificador']['queue'],
    dag=dag,
    lat=_params['lat'],
    lon=_params['lon'],
    params=_steps['clasificador']['params'],
    delete_partial_results=_steps['clasificador']['del_prev_result'],
    to_tiff=True
)


entrenamiento>>clasificador
workflow>>clasificador
sensor_fin_ejecucion = CompressFileSensor(task_id='sensor_fin_ejecucion',poke_interval=60, soft_fail=True,mode='reschedule', queue='util', dag=dag) 
comprimir_resultados = PythonOperator(task_id='comprimir_resultados',provide_context=True,python_callable=other_utils.compress_results,queue='util',op_kwargs={'execID': args['execID']},dag=dag) 
sensor_fin_ejecucion >> comprimir_resultados 
