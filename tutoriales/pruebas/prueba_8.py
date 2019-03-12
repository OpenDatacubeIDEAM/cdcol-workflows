# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (2,4),
	'lon': (-77,-73),
	'time_ranges': ("2017-01-01", "2017-12-31"),
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'products': ["LS8_OLI_LASRC"],
	'mosaic': False
}

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "prueba8",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='prueba8', default_args=args,
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
        queue='airflow_small', dag=dag, taxprefix="masked_{}_".format(_params['products'][0])

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
                                      queue='airflow_small',dag=dag , taxprefix="masked_{}_".format(_params['products'][1])

									   )
	full_query = dag_utils.reduceByTile(masked0 + masked1, algorithm="joiner-reduce", version="1.0", queue='airflow_small', dag=dag,   taxprefix="joined", params={'bands': _params['bands']},)
else:
	full_query = masked0

medians = dag_utils.IdentityMap(
    full_query,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_",
    queue='airflow_small',dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

if _params['mosaic']:
    task_id = 'mosaic'
    algorithm = 'joiner'
    queue = 'airflow_small'


else:
    task_id = 'print_context'
    algorithm = 'test-reduce'
    queue = 'airflow_small'


join = CDColReduceOperator(task_id=task_id,algorithm=algorithm,version='1.0', queue=queue, dag=dag)
map(lambda b: b >> join, medians)