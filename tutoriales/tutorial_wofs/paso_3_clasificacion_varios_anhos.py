import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils, queue_utils


from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,11),
    'lon': (-76,-75),
    'time_ranges': ("2013-01-01", "2014-12-31"),
    'products': ["LS7_ETM_LEDAPS"],
}

_steps = {
    'wofs': {
        'algorithm': "wofs-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(),
    },
}

args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10_paso_3_clasificacion_varios_anhos",
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

wofs