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
    'joiner-reduce-wofs': queue_utils.assign_queue(
        input_type='multi_temporal',
        time_range=_params['time_ranges']),
    'wofs-time-series-wf': queue_utils.assign_queue(
        input_type='multi_temporal',
        time_range=_params['time_ranges']),
    'mosaic': queue_utils.assign_queue(
        input_type='multi_temporal_area',
        time_range=_params['time_ranges'],
        lat=_params['lat'],
        lon=_params['lon']),
}


args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10_paso_4_mosaico_wofs",
    'product':_params['products'][0]
}

dag = DAG(
    dag_id='mp.mancipe10_paso_4_mosaico_wofs', default_args=args,
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

time_series=dag_utils.IdentityMap(
    reducer,
        algorithm="wofs-time-series-wf",
        version="1.0",
        taxprefix="wofs_time_series_",
        queue=_queues['wofs-time-series-wf'],
        dag=dag
)


join = CDColReduceOperator(task_id='mosaic',
                           algorithm='joiner',
                           version='1.0',
                           queue=_queues['mosaic'],
                           dag=dag)
map(lambda b: b >> join, time_series)