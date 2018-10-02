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
    'time_ranges': [("2014-01-01", "2014-12-31")],
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'products': ["LS8_OLI_LASRC", "LS7_ETM_LEDAPS"],
	'mosaic': False
}

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "ndvi",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='ndvi', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

masked0=dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
	time_ranges= _params['time_ranges'],
	algorithm="mascara-landsat", version="1.0",
        product=_params['products'][0],
        params={
                'normalized':_params['normalized'],
                'bands':_params['bands'],
                'minValid': _params['minValid']
        },
        dag=dag, taxprefix="masked_{}_".format(_params['products'][0])

)
if len(_params['products']) > 1:
	masked1 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
									   time_ranges=_params['time_ranges'],
									   algorithm="mascara-landsat", version="1.0",
									   product=_params['products'][1],
									   params={
										   'normalized': _params['normalized'],
										   'bands': _params['bands'],
										   'minValid': _params['minValid']
									   },
									   dag=dag, taxprefix="masked_{}_".format(_params['products'][1])

									   )
	full_query = dag_utils.reduceByTile(masked0 + masked1, algorithm="joiner-reduce", version="1.0", dag=dag,taxprefix="joined")
else:
	full_query = masked0

medians = dag_utils.IdentityMap(
    full_query,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

ndvi=dag_utils.IdentityMap(medians, algorithm="ndvi-wf", version="1.0", dag=dag, taxprefix="ndvi")

if _params['mosaic']:
	task_id = 'mosaic'
	algorithm = 'joiner'

else:
	task_id = 'print_context'
	algorithm = 'test-reduce'

join = CDColReduceOperator(task_id=task_id,algorithm=algorithm,version='1.0',dag=dag)
map(lambda b: b >> join, ndvi)