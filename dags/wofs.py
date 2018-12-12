import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator, PythonOperator
from cdcol_utils import dag_utils, other_utils, queue_utils


from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,11),
    'lon': (-76,-74),
    'time_ranges': ("2013-01-01", "2015-12-31"),
    'products': ["LS8_OLI_LASRC"],
    'mosaic':True,
}

_queues = {

    'wofs-wf': queue_utils.assign_queue(),
    'joiner-reduce-wofs': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'], unidades=len(_params['products'])),
    'wofs-time-series-wf': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'], unidades=len(_params['products'])),
    'joiner': queue_utils.assign_queue(input_type='multi_temporal_unidad_area', time_range=_params['time_ranges'], lat=_params['lat'], lon=_params['lon'], unidades=len(_params['products'])),
}


args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"wofs-wf",
    'product':_params['products'][0]
}

dag = DAG(
    dag_id='wofs-wf', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

wofs_classification = dag_utils.queryMapByTileByMonths(
    lat=_params['lat'],
    lon=_params['lon'],
    time_ranges=_params['time_ranges'],
    algorithm="wofs-wf",
    version="1.0",
    product=_params['products'][0],
    queue= _queues['wofs-wf'],
    months=6,
    dag=dag,
    taxprefix="wofs_"
)

reducer=dag_utils.reduceByTile(wofs_classification, algorithm="joiner",version="1.0",queue=_queues['joiner-reduce-wofs'], dag=dag, taxprefix="joined")

time_series=dag_utils.IdentityMap(
    reducer,
        algorithm="wofs-time-series-wf",
        version="1.0",
        taxprefix="wofs_time_series_",
        queue=_queues['wofs-time-series-wf'],
        dag=dag
)

delete_partial_results = PythonOperator(task_id='delete_partial_results',
                                            provide_context=True,
                                            python_callable=other_utils.delete_partial_results,
                                            queue='airflow_small',
                                            op_kwargs={'algorithms': {
                                                'wofs-wf': "1.0",
                                                'joiner-reduce-wofs': "1.0",
                                            }, 'execID': args['execID']},
                                            dag=dag)

if _params['mosaic']:
    mosaic = dag_utils.OneReduce(time_series, algorithm="joiner", version="1.0", queue=_queues['joiner'], dag=dag, taxprefix="mosaic")
    # if _params['normalized']:
    #     normalization = CDColFromFileOperator(task_id="normalization", algorithm="normalization-wf", version="1.0", queue=_queues['normalization'])

    map(lambda b: b >> delete_partial_results, mosaic)

else:

    map(lambda b: b >> delete_partial_results, time_series)