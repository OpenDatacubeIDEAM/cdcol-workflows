import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {{params}}

_steps = {

    'k_means': {
        'algorithm': "k-means-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_area',
            lat=_params['lat'],
            lon=_params['lon']
        ),
        'params': {'clases': _params['clases']},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    }

}

args = {
    'owner': _params['owner'],
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': _params['execID'],
    'product':_params['products'][0]
}

dag = DAG(
    dag_id=args["execID"], 
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))


kmeans = CDColQueryOperator(
    task_id="k_means",
    time_ranges=_params['time_ranges'][0],
    product=_params['products'][0],
    algorithm=_steps['k_means']['algorithm'],
    version=_steps['k_means']['version'],
    queue=_steps['k_means']['queue'], 
    dag=dag,
    lat=_params['lat'],
    lon=_params['lon'],
    params=_steps['k_means']['params'],
    to_tiff=True
)

kmeans
