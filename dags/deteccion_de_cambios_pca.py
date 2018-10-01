# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (0,2),
    'lon': (-74,-72),
    'time_ranges': [("2014-01-01", "2014-12-31"), ("2015-01-01", "2015-12-31")],
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'classes':4
}


args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "deteccionDeCambiosPCA",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='deteccion_cambios_PCA', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

period1 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'], time_ranges=_params['time_ranges'],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS8_OLI_LASRC",
                                     params={
                                         'normalized': _params['normalized'],
                                         'bands': _params['bands'],
                                         'minValid': _params['minValid'],
                                     },
                                     dag=dag, taxprefix="period1_")

period2 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'], time_ranges=_params['time_ranges'][1],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS8_OLI_LASRC",
                                     params={
                                         'normalized': _params['normalized'],
                                         'bands': _params['bands'],
                                         'minValid': _params['minValid'],
                                     },
                                     dag=dag, taxprefix="period2_")
medians1 = dag_utils.IdentityMap(
   period1,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

medians2 = dag_utils.IdentityMap(
   period2,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

mosaic1 = dag_utils.OneReduce(medians1, algorithm="joiner", version="1.0", dag=dag, taxprefix="mosaic1")

mosaic2 = dag_utils.OneReduce(medians2, algorithm="joiner", version="1.0", dag=dag, taxprefix="mosaic2")

preprocess = dag_utils.reduceByTile(mosaic1+mosaic2, algorithm="preprocess-two", version="1.0", dag=dag, taxprefix="preprocess")

pca = dag_utils.IdentityMap(
   preprocess,
    algorithm="deteccion-cambios-pca-wf",
    version="1.0",
    taxprefix="pca_",
    dag=dag,
)
kmeans = dag_utils.IdentityMap(
    pca,
    algorithm="k-means-wf",
    version="1.0",
    taxprefix="kmeans_",
    dag=dag,
    params={'classes': _classes}
)


reduce= CDColReduceOperator(
    task_id='print_context',
    algorithm='test-reduce',
    version='1.0',
    dag=dag)

map(lambda b: b >> reduce, kmeans)