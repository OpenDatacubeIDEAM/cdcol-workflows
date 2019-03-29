import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

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

_params = {
    'lat': (10, 11),
    'lon': (-75, -74),
    'time_ranges': ("2011-01-01", "2011-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid': 1,
    'normalized': True,
    'modelos': '/web_storage/downloads/models/',
    'products': ["LS7_ETM_LEDAPS"],
    'genera_mosaico': True,
    'genera_geotiff': True,
    'elimina_resultados_anteriores': True
}

_steps = {
    'mascara': {
        'algorithm': "mascara-landsat",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal', time_range=_params['time_ranges']),
        'params': {'bands': _params['bands']},
    },
    'reduccion': {
        'algorithm': "joiner-reduce",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'],
                                          unidades=len(_params['products'])),
        'params': {'bands': _params['bands']},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'medianas': {
        'algorithm': "compuesto-temporal-medianas-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'],
                                          unidades=len(_params['products'])),
        'params': {
            'normalized': _params['normalized'],
            'bands': _params['bands'],
            'minValid': _params['minValid'],
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'mosaico': {
        'algorithm': "joiner",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area', lat=_params['lat'], lon=_params['lon']),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'clasificador': {
        'algorithm': "clasificador-generico-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area', lat=_params['lat'], lon=_params['lon']),
        'params': {
            'bands': _params['bands'],
            'modelos': _params['modelos'] + args["execID"]
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'geotiff': {
        'algorithm': "generate-geotiff",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area', lat=_params['lat'], lon=_params['lon']),
        'params': {},
        'del_prev_result': False,
    }

}

mascara_0 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                     time_ranges=_params['time_ranges'],
                                     algorithm=_steps['mascara']['algorithm'], version=_steps['mascara']['version'],
                                     product=_params['products'][0],
                                     params=_steps['mascara']['params'],
                                     queue=_steps['mascara']['queue'], dag=dag,
                                     task_id="mascara_" + _params['products'][0])

if len(_params['products']) > 1:
    mascara_1 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                         time_ranges=_params['time_ranges'],
                                         algorithm=_steps['mascara']['algorithm'],
                                         version=_steps['mascara']['version'],
                                         product=_params['products'][1],
                                         params=_steps['mascara']['params'],
                                         queue=_steps['mascara']['queue'], dag=dag,
                                         task_id="mascara_" + _params['products'][1])

    reduccion = dag_utils.reduceByTile(mascara_0 + mascara_1, algorithm=_steps['reduccion']['algorithm'],
                                       version=_steps['reduccion']['version'],
                                       queue=_steps['reduccion']['queue'], dag=dag, task_id="joined",
                                       delete_partial_results=_steps['reduccion']['del_prev_result'],
                                       params=_steps['reduccion']['params'], )
else:
    reduccion = mascara_0

medianas = dag_utils.IdentityMap(
    reduccion,
    algorithm=_steps['medianas']['algorithm'],
    version=_steps['medianas']['version'],
    task_id="medianas",
    queue=_steps['medianas']['queue'], dag=dag,
    delete_partial_results=_steps['medianas']['del_prev_result'],
    params=_steps['medianas']['params'])

workflow=medianas
if queue_utils.get_tiles(_params['lat'],_params['lon'])>1:
    mosaico = dag_utils.OneReduce(workflow, task_id="mosaic", algorithm=_steps['mosaico']['algorithm'],
                                  version=_steps['mosaico']['version'], queue=_steps['mosaico']['queue'],
                                  delete_partial_results=_steps['mosaico']['del_prev_result'],
                                  trigger_rule=TriggerRule.NONE_FAILED, dag=dag)
    workflow=mosaico

entrenamiento = dag_utils.IdentityMap(
    workflow,
    algorithm="random-forest-training",
    version=_steps['medianas']['version'],
    task_id="entrenamiento",
    queue=_steps['medianas']['queue'], dag=dag,
    delete_partial_results=_steps['mosaico']['del_prev_result'],
    params={
        'bands': _params['bands'],
        'train_data_path': _params['modelos'] + args["execID"]
    })

entrenamiento

# clasificador = CDColFromFileOperator(task_id="clasificador_generico", algorithm=_steps['clasificador']['algorithm'], version=_steps['clasificador']['version'], queue=_steps['clasificador']['queue'], dag=dag,  lat=_params['lat'], lon=_params['lon'], params=_steps['clasificador']['params'])
#
# workflow = [mosaico >> clasificador]
#
# if _steps['clasificador']['del_prev_result']:
#     eliminar_mosaico = PythonOperator(task_id="del_"+mosaico[0].task_id,
#                                 provide_context=True,
#                                 python_callable=other_utils.delete_partial_result,
#                                 queue='airflow_small',
#                                 op_kwargs={'algorithm': mosaico[0].algorithm, 'version':mosaico[0].version, 'execID': args['execID'], 'task_id':mosaico[0].task_id},
#                                 dag=dag)
#     eliminar_mosaico = workflow >> eliminar_mosaico
#
# if _params['genera_geotiff']:
#     geotiff = dag_utils.BashMap(workflow, task_id="generate-geotiff", algorithm=_steps['geotiff']['algorithm'],
#                                 version=_steps['geotiff']['version'],
#                                 queue=_steps['geotiff']['queue'],
#                                 delete_partial_results=_steps['geotiff']['del_prev_result'], dag=dag)
#
# eliminar_mosaico
# workflow

