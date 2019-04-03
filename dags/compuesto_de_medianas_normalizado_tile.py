import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9, 11),
    'lon': (-76, -74),
    'time_ranges': ("2017-01-01", "2017-12-31"),
    'bands': ["red", "nir", "swir1", "swir2"],
    'minValid': 1,
    'products': ["LS8_OLI_LASRC", "LS7_ETM_LEDAPS_MOSAIC"],
    'genera_mosaico': True,
    'genera_geotiff': True,
    'elimina_resultados_anteriores': False
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
        'queue': queue_utils.assign_queue(input_type='multi_temporal_unidad',
                                          time_range=_params['time_ranges'],
                                          unidades=len(_params['products'])),
        'params': {
            'bands': _params['bands'],
            'minValid': _params['minValid'],
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'normalizacion': {
        'algorithm': "normalizacion",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_unidad',
                                          unidades=len(_params['products'])),
        'params': {
            'Bands': _params['bands'],
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
    'geotiff': {
        'algorithm': "generate-geotiff",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area', lat=_params['lat'], lon=_params['lon']),
        'params': {},
        'del_prev_result': False,
    }

}

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "compuesto_de_medianas_normalizado_tile",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id=args['execID'], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

mascara_ls8 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                       time_ranges=_params['time_ranges'],
                                       algorithm=_steps['mascara']['algorithm'],
                                       version=_steps['mascara']['version'],
                                       product=_params['products'][0],
                                       params=_steps['mascara']['params'],
                                       queue=_steps['mascara']['queue'], dag=dag,
                                       task_id="consulta_cubo_" + _params['products'][0])

mascara_ls7_mosaic = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                              time_ranges=_params['time_ranges'],
                                              algorithm='just-query',
                                              version=_steps['mascara']['version'],
                                              product=_params['products'][1],
                                              params=_steps['mascara']['params'],
                                              queue=_steps['mascara']['queue'],
                                              dag=dag,
                                              task_id="consulta_referencia_" + _params['products'][1])

medianas = dag_utils.IdentityMap(
    mascara_ls8,
    algorithm=_steps['medianas']['algorithm'],
    version=_steps['medianas']['version'],
    task_id="medianas",
    queue=_steps['medianas']['queue'], dag=dag,
    delete_partial_results=_steps['medianas']['del_prev_result'],
    params=_steps['medianas']['params'])

normalizacion = dag_utils.reduceByTile(medianas + [mascara_ls7_mosaic],
                                       algorithm=_steps['normalizacion']['algorithm'],
                                       version=_steps['normalizacion']['version'],
                                       queue=_steps['normalizacion']['queue'],
                                       params=_steps['normalizacion']['params'],
                                       delete_partial_results=_steps['normalizacion']['del_prev_result'],
                                       dag=dag, task_id="normalizacion")

workflow = normalizacion
if _params['genera_mosaico']:
    mosaico = dag_utils.OneReduce(workflow, task_id="mosaic",
                                  algorithm=_steps['mosaico']['algorithm'],
                                  version=_steps['mosaico']['version'],
                                  queue=_steps['mosaico']['queue'],
                                  delete_partial_results=_steps['mosaico']['del_prev_result'],
                                  trigger_rule=TriggerRule.NONE_FAILED, dag=dag)
    workflow = mosaico

if _params['genera_geotiff']:
    geotiff = dag_utils.BashMap(workflow, task_id="generate-geotiff",
                                algorithm=_steps['geotiff']['algorithm'],
                                version=_steps['geotiff']['version'],
                                queue=_steps['geotiff']['queue'],
                                delete_partial_results=_steps['geotiff']['del_prev_result'], dag=dag)
    workflow = geotiff

workflow
