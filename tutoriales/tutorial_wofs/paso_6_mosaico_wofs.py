import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils, queue_utils
from airflow.utils.trigger_rule import TriggerRule


from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,11),
    'lon': (-76,-75),
    'time_ranges': ("2012-01-01", "2013-12-31"),
    'products': ["LS7_ETM_LEDAPS"],
    'genera_mosaico': True,
    'genera_geotiff': True,
    'elimina_resultados_anteriores': True
}

_steps = {
    'wofs': {
        'algorithm': "wofs-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(),
    },
    'reduccion': {
        'algorithm': "joiner",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'],
                                          unidades=len(_params['products'])),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'serie_tiempo': {
        'algorithm': "wofs-time-series-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal_unidad',
            time_range=_params['time_ranges'],
            unidades=len(_params['products'])),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'mosaico': {
        'algorithm': "joiner",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_area',
            lat=_params['lat'],
            lon=_params['lon']),
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'geotiff': {
        'algorithm': "generate-geotiff",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_area',
            lat=_params['lat'],
            lon=_params['lon']),
        'params': {},
        'del_prev_result': False,
    }
}

args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10_wofs_paso_6_mosaico_wofs",
    'product':_params['products'][0]
}

dag = DAG(
    dag_id=args['execID'], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

wofs = dag_utils.queryMapByTileByYear( lat=_params['lat'], lon=_params['lon'],
    time_ranges=_params['time_ranges'], product=_params['products'][0],
    algorithm=_steps['wofs']['algorithm'], version=_steps['wofs']['version'],
    queue=_steps['wofs']['queue'],
    dag=dag, task_id="wofs"
)

reduccion = dag_utils.reduceByTile(wofs, algorithm=_steps['reduccion']['algorithm'],
                                       version=_steps['reduccion']['version'],
                                       queue=_steps['reduccion']['queue'], dag=dag, task_id="joined",
                                       delete_partial_results=_steps['reduccion']['del_prev_result'],
                                       params=_steps['reduccion']['params'], )

serie_tiempo=dag_utils.IdentityMap( reduccion, algorithm=_steps['serie_tiempo']['algorithm'],
        version=_steps['serie_tiempo']['version'], task_id="wofs_serie_tiempo",
        queue=_steps['serie_tiempo']['queue'], delete_partial_results=_steps['serie_tiempo']['del_prev_result'],
        dag=dag
)

workflow = serie_tiempo
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
                                delete_partial_results=_steps['geotiff']['del_prev_result'],
                                dag=dag)
    workflow = geotiff

workflow