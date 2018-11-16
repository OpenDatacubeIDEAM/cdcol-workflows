import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils, queue_utils


from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,11),
    'lon': (-76,-75),
    'time_ranges': ("2013-01-01", "2015-12-31"),
    'products': ["LS8_OLI_LASRC"],
}

_queues = {

    'wofs-wf': queue_utils.assign_queue(),
    'joiner-reduce-wofs': queue_utils.assign_queue(input_type='multi_temporal_unidad',time_range=_params['time_ranges'],unidades=len(_params['products'])),

}


args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"df.nino10_paso_1_clasificacion",
    'product':_params['products'][0]
}

dag = DAG(
    dag_id='df.nino10_paso_1_clasificacion', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

wofs_classification = dag_utils.queryMapByTileByYear(
    lat=_params['lat'],
    lon=_params['lon'],
    time_ranges=_params['time_ranges'],
    algorithm="wofs-wf",
    version="1.0",
    product=_params['products'][0],
    queue=_queues['wofs-wf'],
    dag=dag,
    taxprefix="wofs_"
)

reducer=dag_utils.reduceByTile(wofs_classification, algorithm="joiner-reduce-wofs",version="1.0",queue=_queues['joiner-reduce-wofs'], dag=dag, taxprefix="joined")

reducer