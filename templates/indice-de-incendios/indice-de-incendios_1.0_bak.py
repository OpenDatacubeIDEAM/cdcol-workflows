"""
Modificado por Crhsitian Segura
Fecha: 24/09/2020
Desc: Revision y modificaciones debido que no estaba funcionando el workflow original (nbr_1.0_old.py).

"""


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

_params = {{params}}
#_params = {'minValid': 1, 'normalized': True, 'lat': (2, 4), 'lon': (-74, -73), 'products': [{'name': 'LS7_ETM_LEDAPS', 'bands': ['red', 'green', 'nir', 'radsat_qa', 'swir1', 'blue', 'atmos_opacity', 'pixel_qa', 'swir2', 'cloud_qa']},{'name': 'LS8_OLI_LASRC', 'bands': ['red', 'green', 'nir', 'swir1', 'blue', 'pixel_qa', 'swir2']}], 'time_ranges': [('2019-01-01', '2019-01-31')], 'execID': 'NBR_test6', 'elimina_resultados_anteriores': True, 'genera_mosaico': True, 'owner': 'API-REST'}

''' 
# Commented by Crhistian, the template has not params
# Changed on 23-sept-2020
# This NBR is not on the gitHub repository

_params = {
    'lat': (4, 6),
    'lon': (-74, -72),
    'time_ranges': ("2017-01-01", "2017-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2", "pixel_qa"],
    'minValid': 1,
    'products': ["LS8_OLI_LASRC"],
    'genera_mosaico': True,
    'elimina_resultados_anteriores': True
}
'''

_steps = {
    'mascara': {
        'algorithm': "mascara-landsat",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal', time_range=_params['time_ranges'][0]),
        'params': {},
    },
    'reduccion': {
        #'algorithm': "joiner-reduce",
        'algorithm': "joiner",
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
            'minValid': _params['minValid'],
            'normalized': _params['normalized'],
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'savi': {
        'algorithm': "savi-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(),
        'params': {},
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
    dagrun_timeout=timedelta(minutes=120))

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
    task_id="mascara_" + _params['products'][0]['name'])


if len(_params['products']) > 1:
    mascara_1 = dag_utils.queryMapByTile(
	lat=_params['lat'], lon=_params['lon'],
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
        algorithm=_steps['reduccion']['algorithm'],
        version=_steps['reduccion']['version'],
        queue=_steps['reduccion']['queue'],
        product=_params['products'][0],
        dag=dag,
        task_id="joined",
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

savi = dag_utils.IdentityMap(medianas, algorithm=_steps['savi']['algorithm'],
                             version=_steps['savi']['version'],
                             queue=_steps['savi']['queue'],
                             delete_partial_results=_steps['savi']['del_prev_result'], dag=dag,
                             task_id="savi", to_tiff= not _params['genera_mosaico'])

workflow = savi
if _params['genera_mosaico']:
    mosaico = dag_utils.OneReduce(workflow, task_id="mosaic", algorithm=_steps['mosaico']['algorithm'],
                                  version=_steps['mosaico']['version'], queue=_steps['mosaico']['queue'],
                                  delete_partial_results=_steps['mosaico']['del_prev_result'],
                                  trigger_rule=TriggerRule.NONE_FAILED, dag=dag, to_tiff=True)

    workflow = mosaico

workflow
sensor_fin_ejecucion = CompressFileSensor(task_id='sensor_fin_ejecucion',poke_interval=60, soft_fail=True,mode='reschedule', queue='util', dag=dag) 
comprimir_resultados = PythonOperator(task_id='comprimir_resultados',provide_context=True,python_callable=other_utils.compress_results,queue='util',op_kwargs={'execID': args['execID']},dag=dag) 
sensor_fin_ejecucion >> comprimir_resultados 

