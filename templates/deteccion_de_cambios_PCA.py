from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {{params}}

_steps = {
    'mascara': {
        'algorithm': "mascara-landsat",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal', time_range=_params['time_ranges'][0]),
        'params': {},
    },
    'reduccion': {
        'algorithm': "joiner-reduce",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'][0],
                                          unidades=len(_params['products'])),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'medianas': {
        'algorithm': "compuesto-temporal-medianas-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'][0],
                                          unidades=len(_params['products'])),
        'params': {
            'normalized':_params['normalized'],
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
    'pca': {
        'algorithm': "deteccion-cambios-pca-wf",
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
    'owner': _params['owner'],
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': _params['execID'],
    'product': _params['products'][0]
}

dag = DAG(
    dag_id=args["execID"], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

mascara_periodo_1 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                     time_ranges=_params['time_ranges'][0],
                                     algorithm=_steps['mascara']['algorithm'], version=_steps['mascara']['version'],
                                     product=_params['products'][0],
                                     params=_steps['mascara']['params'],
                                     queue=_steps['mascara']['queue'], dag=dag,
                                     task_id="mascara_p1_" + _params['products'][0]['name'])

mascara_periodo_2 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                     time_ranges=_params['time_ranges'][1],
                                     algorithm=_steps['mascara']['algorithm'], version=_steps['mascara']['version'],
                                     product=_params['products'][0],
                                     params=_steps['mascara']['params'],
                                     queue=_steps['mascara']['queue'], dag=dag,
                                     task_id="mascara_p2_" + _params['products'][0]['name'])

medianas_periodo_1 = dag_utils.IdentityMap(
    mascara_periodo_1,
    algorithm=_steps['medianas']['algorithm'],
    product=_params['products'][0],
    version=_steps['medianas']['version'],
    task_id="medianas_p1_",
    queue=_steps['medianas']['queue'], dag=dag,
    delete_partial_results=_steps['medianas']['del_prev_result'],
    params=_steps['medianas']['params'])

medianas_periodo_2 = dag_utils.IdentityMap(
    mascara_periodo_2,
    algorithm=_steps['medianas']['algorithm'],
    product=_params['products'][0],
    version=_steps['medianas']['version'],
    task_id="medianas_p2_",
    queue=_steps['medianas']['queue'], dag=dag,
    delete_partial_results=_steps['medianas']['del_prev_result'],
    params=_steps['medianas']['params'])

if queue_utils.get_tiles(_params['lat'],_params['lon'])>1:
    mosaico_periodo_1 = dag_utils.OneReduce(medianas_periodo_1, task_id="mosaico_p1_", algorithm=_steps['mosaico']['algorithm'],
                                  version=_steps['mosaico']['version'], queue=_steps['mosaico']['queue'],
                                  delete_partial_results=_steps['mosaico']['del_prev_result'],
                                  trigger_rule=TriggerRule.NONE_FAILED, dag=dag)

    mosaico_periodo_2 = dag_utils.OneReduce(medianas_periodo_2, task_id="mosaico_p2_",
                                            algorithm=_steps['mosaico']['algorithm'],
                                            version=_steps['mosaico']['version'], queue=_steps['mosaico']['queue'],
                                            delete_partial_results=_steps['mosaico']['del_prev_result'],
                                            trigger_rule=TriggerRule.NONE_FAILED, dag=dag)

    resultado_completo = mosaico_periodo_1+mosaico_periodo_2
else:
    resultado_completo = medianas_periodo_1+medianas_periodo_2


pca = dag_utils.reduceByTile(resultado_completo,
                             task_id="pca",
                             algorithm=_steps['pca']['algorithm'],
                             version=_steps['pca']['version'],
                             queue=_steps['pca']['queue'],
                             product=_params['products'][0],
                             dag=dag,
                             delete_partial_results=_steps['pca']['del_prev_result'],
                             params=_steps['pca']['params'], to_tiff=True)

pca

