#coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils, queue_utils

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (9,11),
    'lon': (-76, -75),
    'time_ranges': ("2013-01-01", "2013-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'classes':4,
    'products': ["LS8_OLI_LASRC", "LS7_ETM_LEDAPS"],
}

args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"mp.mancipe10-paso-8-multiunidad-k-means-utils",
    'product':_params['products'][0]
}

dag = DAG(
    dag_id='mp.mancipe10-paso-8-multiunidad-k-means-utils', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

consulta_ls8=dag_utils.queryMapByTile(lat=_params['lat'],
                                      lon=_params['lon'],
                                      time_ranges= _params['time_ranges'],
                                      algorithm="mascara-landsat",
                                      version="1.0",
                                      product=_params['products'][0],
                                      params={'bands':_params['bands']},
                                      queue='airflow_small', dag=dag, taxprefix="masked_{}_".format(_params['products'][0]))

consulta_ls7 = dag_utils.queryMapByTile(lat=_params['lat'],
                                        lon=_params['lon'],
                                        time_ranges=_params['time_ranges'],
                                        algorithm="mascara-landsat",
                                        version="1.0",
                                        product=_params['products'][1],
                                        params={'bands': _params['bands']},
                                        queue='airflow_small',dag=dag, taxprefix="masked_{}_".format(_params['products'][1]))

reducer = dag_utils.reduceByTile(consulta_ls7+consulta_ls8,
                                 algorithm="joiner-reduce",
                                 version="1.0",
                                 queue='airflow_medium',
                                 dag=dag,
                                 taxprefix="joined" ,
                                 params={'bands': _params['bands']})


medianas = dag_utils.IdentityMap(reducer,
                                 algorithm="compuesto-temporal-medianas-wf",
                                 version="1.0",
                                 taxprefix="medianas_",
                                 queue='airflow_small',
                                 dag=dag,
                                 params={
                                     'normalized': _params['normalized'],
                                     'bands': _params['bands'],
                                     'minValid': _params['minValid'],
                                 })


mosaico = dag_utils.OneReduce(medianas,
                              algorithm="joiner",
                              version="1.0",
                              queue='airflow_medium',
                              dag=dag, taxprefix="mosaic")

kmeans = dag_utils.IdentityMap(mosaico,
                               algorithm="k-means-wf",
                               version="1.0",
                               taxprefix="kmeans_",
                               queue='airflow_medium',
                               dag=dag,
                               params={'classes': _params['classes']})

kmeans