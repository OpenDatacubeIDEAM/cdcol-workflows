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
    'time_ranges': ("2015-01-01", "2015-12-31"),
    'bands': ["red", "nir", "swir1", "swir2"],
    'minValid': 1,
    'normalized': True,
    'products': ["LS8_OLI_LASRC","LS7_ETM_LEDAPS_MOSAIC"],
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
    'execID': "compuesto_de_medianas_normalizado",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id=args['execID'], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

mascara_ls8 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                     time_ranges=_params['time_ranges'],
                                     algorithm=_steps['mascara']['algorithm'], version=_steps['mascara']['version'],
                                     product=_params['products'][0],
                                     params=_steps['mascara']['params'],
                                     queue=_steps['mascara']['queue'], dag=dag,
                                     task_id="consulta_cubo_" + _params['products'][0])

mascara_ls7_mosaic = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                     time_ranges=_params['time_ranges'],
                                     algorithm='just_query', version=_steps['mascara']['version'],
                                     product=_params['products'][1],
                                     params=_steps['mascara']['params'],
                                     queue=_steps['mascara']['queue'], dag=dag,
                                     task_id="consulta_referencia_" + _params['products'][1])



medianas = dag_utils.IdentityMap(
    mascara_ls8,
    algorithm=_steps['medianas']['algorithm'],
    version=_steps['medianas']['version'],
    task_id="medianas",
    queue=_steps['medianas']['queue'], dag=dag,
    delete_partial_results=_steps['medianas']['del_prev_result'],
    params=_steps['medianas']['params'])


normalizacion = dag_utils.reduceByTile(medianas+mascara_ls7_mosaic, algorithm="normalizacion",
                          version="1.0", queue=_steps['mosaico']['queue'],
                          params={
                              'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
                          },
                          dag=dag, task_id="normalizacion", )


mosaico = dag_utils.OneReduce(normalizacion, task_id="mosaic", algorithm=_steps['mosaico']['algorithm'],
                                  version=_steps['mosaico']['version'], queue=_steps['mosaico']['queue'],
                                  delete_partial_results=_steps['mosaico']['del_prev_result'],
                                  trigger_rule=TriggerRule.NONE_FAILED, dag=dag)



mosaico