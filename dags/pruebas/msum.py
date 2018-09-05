# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from dags.cdcol_utils import dag_utils


from datetime import timedelta
from pprint import pprint
_lat=(2,4)
_lon=(-69,-67)

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"primeraPruebaMSUM",
    'product':"Multiple"
}


dag = DAG(
    dag_id='multi_storage_unit_medians', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15))

maskedLS8 = dag_utils.queryMapByTile( lat=_lat, 
        lon=_lon,time_ranges=[("2013-01-01","2013-12-31")],
        algorithm="mascara-landsat",version="1.0",
        product="LS8_OLI_LASRC",
        params={
            'normalized':True,
            'bands':["blue","green","red","nir", "swir1","swir2"], 
            'minValid':1,
        },
        dag=dag, taxprefix="maskedLS8_")

maskedLS7 = dag_utils.queryMapByTile( lat=_lat, 
        lon=_lon,time_ranges=[("2013-01-01","2013-12-31")],
        algorithm="mascara-landsat",version="1.0",
        product="LS7_ETM_LEDAPS",
        params={
            'normalized':True,
            'bands':["blue","green","red","nir", "swir1","swir2"], 
            'minValid':1,
        },
        dag=dag, taxprefix="maskedLS7_")

joins=dag_utils.reduceByTile(maskedLS7+maskedLS8, algorithm="joiner-reduce",version="1.0",dag=dag, taxprefix="joined")
medians=dag_utils.IdentityMap(
        joins,
        algorithm="compuesto-temporal-medianas-wf",
        version="1.0",
        taxprefix="medianas_",
        dag=dag,
        params={
            'normalized':True,
            'bands':["blue","green","red","nir", "swir1","swir2"], 
            'minValid':1,
        })

        
reduce= CDColReduceOperator(
    task_id='print_context',
    algorithm='test-reduce',
    version='1.0',
    dag=dag)

map(lambda b: b>>reduce,medians)