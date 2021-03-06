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
    'lon': (-76, -75),
    'time_ranges': [("2014-01-01", "2014-12-31"), ("2015-01-01", "2015-12-31")],
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2", "pixel_qa"],
    'minValid': 1,
    'normalized': True,
    'products': ["LS7_ETM_LEDAPS"],
    'elimina_resultados_anteriores': True
}

_steps = {
    'mascara': {
        'algorithm': "mascara-landsat",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal', time_range=_params['time_ranges'][0]),
        'params': {'bands': _params['bands']},
    },
    'medianas': {
        'algorithm': "compuesto-temporal-medianas-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal_unidad',
            time_range=_params['time_ranges'][0],
            unidades=len(_params['products'])),
        'params': {
            'normalized': _params['normalized'],
            'bands': _params['bands'],
            'minValid': _params['minValid'],
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    }

}

args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "mp.mancipe10_pca_paso_5_medianas_varios_tiles",
    'product': _params['products'][0]
}

dag = DAG(
    dag_id=args["execID"], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

mascara_periodo_1 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                             time_ranges=_params['time_ranges'][0],
                                             algorithm=_steps['mascara']['algorithm'],
                                             version=_steps['mascara']['version'],
                                             product=_params['products'][0],
                                             params=_steps['mascara']['params'],
                                             queue=_steps['mascara']['queue'], dag=dag,
                                             task_id="mascara_p1_" + _params['products'][0])

mascara_periodo_2 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                             time_ranges=_params['time_ranges'][1],
                                             algorithm=_steps['mascara']['algorithm'],
                                             version=_steps['mascara']['version'],
                                             product=_params['products'][0],
                                             params=_steps['mascara']['params'],
                                             queue=_steps['mascara']['queue'], dag=dag,
                                             task_id="mascara_p2_" + _params['products'][0])

medianas_periodo_1 = dag_utils.IdentityMap(
    mascara_periodo_1,
    algorithm=_steps['medianas']['algorithm'],
    version=_steps['medianas']['version'],
    task_id="medianas_p1_",
    queue=_steps['medianas']['queue'], dag=dag,
    delete_partial_results=_steps['medianas']['del_prev_result'],
    params=_steps['medianas']['params'])

medianas_periodo_2 = dag_utils.IdentityMap(
    mascara_periodo_2,
    algorithm=_steps['medianas']['algorithm'],
    version=_steps['medianas']['version'],
    task_id="medianas_p2_",
    queue=_steps['medianas']['queue'], dag=dag,
    delete_partial_results=_steps['medianas']['del_prev_result'],
    params=_steps['medianas']['params'])

medianas_periodo_1
medianas_periodo_2