import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils


from datetime import timedelta
from pprint import pprint

_params = {
     'lat': (9,11),
    'lon': (-76,-74),
    'time_ranges': [("2013-01-01", "2014-12-31")],
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
}
args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"wofs_prueba",
    'product':"LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='wofs_prueba', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

queryLS8 = dag_utils.queryMapByTileByYear(
    lat=_params['lat'],
    lon=_params['lon'],
    time_ranges=_params['time_ranges'],
    algorithm="just-query",
    version="1.0",
    product="LS8_OLI_LASRC",
    params={
        'normalized':_params['normalized'],
        'bands':_params['bands'],
        'minValid':_params['minValid'],
    },
    dag=dag,
    taxprefix="queryLS8_"
)


wofs_classification=dag_utils.IdentityMap(
        queryLS8,
        algorithm="wofs-wf",
        version="1.0",
        taxprefix="wofs_",
        dag=dag)

joins=dag_utils.reduceByTile(wofs_classification, algorithm="joiner-reduce",version="1.0",dag=dag, taxprefix="joined")

time_series=dag_utils.IdentityMap(
    joins,
        algorithm="wofs-time-series-wf",
        version="1.0",
        taxprefix="wofs_time_series_",
        dag=dag
)

reduce= CDColReduceOperator(
    task_id='print_context',
    algorithm='test-reduce',
    version='1.0',
    dag=dag)

map(lambda b: b>>reduce,time_series)