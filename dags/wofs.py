import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils
from cdcol_utils import queue_utils as qu


from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,10),
    'lon': (-76,-75),
    'time_ranges': ("2013-01-01", "2015-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'mosaic':True,
}

_queues = {

    'wofs-wf': qu.get_queue_by_year(time_range=_params['time_ranges'], entrada_multi_temporal=False, tiles=1 ),
    'joiner-reduce-wofs': qu.get_queue_by_year(time_range=_params['time_ranges'], entrada_multi_temporal=True, tiles=1 ),
    'wofs-time-series-wf': qu.get_queue_by_year(time_range=_params['time_ranges'], entrada_multi_temporal=True, tiles=1 ),
    'mosaic': qu.get_queue_by_year(time_range=_params['time_ranges'], entrada_multi_temporal=False, tiles=(_params['lat'][1] - _params['lat'][0])*(_params['lon'][1] - _params['lon'][0]) ),
    'test-reduce': qu.get_queue_by_year(time_range=_params['time_ranges'], entrada_multi_temporal=False, tiles=1),
}


args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"wofs-wf",
    'product':"LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='wofs-wf', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

wofs_classification = dag_utils.queryMapByTileByYear(
    lat=_params['lat'],
    lon=_params['lon'],
    time_ranges=_params['time_ranges'],
    algorithm="wofs-wf",
    version="1.0",
    product="LS8_OLI_LASRC",
    queue= _queues['wofs-wf'],
    dag=dag,
    taxprefix="wofs_"
)

reducer=dag_utils.reduceByTile(wofs_classification, algorithm="joiner-reduce-wofs",version="1.0",queue=_queues['joiner-reduce-wofs'], dag=dag, taxprefix="joined", params={'bands': _params['bands']})

time_series=dag_utils.IdentityMap(
    reducer,
        algorithm="wofs-time-series-wf",
        version="1.0",
        taxprefix="wofs_time_series_",
        queue=_queues['wofs-time-series-wf'],
        dag=dag
)

if _params['mosaic']:
    queue = _queues['mosaic']
    task_id = 'mosaic'
    algorithm = 'joiner'

else:
    queue = _queues['print_context']
    task_id = 'print_context'
    algorithm = 'test-reduce'

join = CDColReduceOperator(task_id=task_id,algorithm=algorithm,version='1.0',queue=queue,dag=dag)
map(lambda b: b >> join, time_series)