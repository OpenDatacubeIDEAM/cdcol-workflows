import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (4, 6),
    'lon': (-74, -72),
    'time_ranges': ("2017-01-01", "2017-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2", "pixel_qa"],
    'minValid': 1,
    'normalized': True,
    'products': ["LS8_OLI_LASRC"],
    'genera_mosaico': True,
    'genera_geotiff': True,
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
        'del_prev_result':True,
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
        'del_prev_result':True,
    },
    'mosaico': {
        'algorithm': "joiner",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area', lat=_params['lat'], lon=_params['lon']),
        'params': {},
        'del_prev_result':True,
    },
    'geotiff':{
        'algorithm': "generate-geotiff",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area', lat=_params['lat'], lon=_params['lon']),
        'params': {},
        'del_prev_result':False,
    }

}


args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "compuesto_de_medianas",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id=args['execID'], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

mascara_0 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                   time_ranges=_params['time_ranges'],
                                   algorithm=_steps['mascara']['algorithm'], version=_steps['mascara']['algorithm'],
                                   product=_params['products'][0],
                                   params=_steps['mascara']['params'],
                                   queue=_steps['mascara']['queue'], dag=dag,
                                   task_id="mascara_" + _params['products'][0])

if len(_params['products']) > 1:
    mascara_1 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                         time_ranges=_params['time_ranges'],
                                         algorithm=_steps['mascara']['algorithm'],
                                         version=_steps['mascara']['algorithm'],
                                         product=_params['products'][1],
                                         params=_steps['mascara']['params'],
                                         queue=_steps['mascara']['queue'], dag=dag,
                                         task_id="mascara_" + _params['products'][1])

    reduccion = dag_utils.reduceByTile(mascara_0+mascara_1, algorithm=_steps['reduccion']['algorithm'], version=_steps['reduccion']['version'],
                                        queue=_steps['reduccion']['queue'], dag=dag, task_id="joined",
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

workflow = medianas
if _params['genera_mosaico']:
    mosaico = dag_utils.OneReduce(workflow, task_id="mosaic", algorithm=_steps['mosaico']['algorithm'], version=_steps['mosaico']['version'], queue=_steps['mosaico']['queue'], delete_partial_results=_steps['mosaico']['del_prev_result'], trigger_rule=TriggerRule.NONE_FAILED, dag=dag)
    # if _params['normalized']:
    #     normalization = CDColFromFileOperator(task_id="normalization", algorithm="normalization-wf", version="1.0", queue=_queues['normalization'])
    workflow = mosaico

if _params['genera_geotiff']:
    geotiff = dag_utils.BashMap(workflow, task_id="generate-geotiff", algorithm=_steps['geotiff']['algorithm'], version=_steps['geotiff']['version'],
                                 queue=_steps['geotiff']['queue'], delete_partial_results=_steps['geotiff']['del_prev_result'], dag=dag)
    workflow = geotiff

workflow
