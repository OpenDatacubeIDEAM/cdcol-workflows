# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils

from datetime import timedelta
from pprint import pprint

_lat = (2, 4)
_lon = (-69, -67)

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "kmeans",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='kmeans', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15))

period1 = dag_utils.queryMapByTile(lat=_lat,
                                     lon=_lon, time_ranges=[("2013-01-01", "2013-12-31")],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS8_OLI_LASRC",
                                     params={
                                         'normalized': True,
                                         'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
                                         'minValid': 1,
                                     },
                                     dag=dag, taxprefix="period1_")

period2 = dag_utils.queryMapByTile(lat=_lat,
                                     lon=_lon, time_ranges=[("2014-01-01", "2014-12-31")],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS8_OLI_LASRC",
                                     params={
                                         'normalized': True,
                                         'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
                                         'minValid': 1,
                                     },
                                     dag=dag, taxprefix="period2_")
medians1 = dag_utils.IdentityMap(
   period1,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    dag=dag,
    params={
        'normalized': True,
        'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
        'minValid': 1,
    })

medians2 = dag_utils.IdentityMap(
   period2,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    dag=dag,
    params={
        'normalized': True,
        'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
        'minValid': 1,
    })

mosaic = CDColReduceOperator(
    task_id='print_context',
    algorithm='joiner',
    version='1.0',
    dag=dag)

map(lambda b: b >> mosaic, medians)