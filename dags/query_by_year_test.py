# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils


from datetime import timedelta
from pprint import pprint

_lat=(2,4)
_lon=(-69,-67)

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"queryByYearTest",
    'product':"queryByYear"
}

dag = DAG(
    dag_id='query_by_year', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=30))

queryLS8 = dag_utils.queryMapByTileByYear( lat=_lat,
        lon=_lon,time_ranges=[("2013-01-01","2014-12-31")],
        algorithm="just-query",version="1.0",
        product="LS8_OLI_LASRC",
        params={
            'normalized':True,
            'bands':["blue","green","red","nir", "swir1","swir2"],
            'minValid':1,
        },
        dag=dag, taxprefix="queryLS8_")
wofs_classification=dag_utils.IdentityMap(
        queryLS8,
        algorithm="wofs-wf",
        version="1.0",
        taxprefix="wofs_",
        dag=dag,
        params={
            'normalized':True,
            'bands':["blue","green","red","nir", "swir1","swir2"],
            'minValid':1,
        })

joins=dag_utils.reduceByTile(wofs_classification, algorithm="joiner-reduce",version="1.0",dag=dag, taxprefix="joined")

reduce= CDColReduceOperator(
    task_id='print_context',
    algorithm='test-reduce',
    version='1.0',
    dag=dag)

map(lambda b: b>>reduce,wofs_classification)