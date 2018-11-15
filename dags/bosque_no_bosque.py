#coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils, queue_utils
from datetime import timedelta
from pprint import pprint

_params = {
	'lat': (9,10),
	'lon': (-76,-75),
	'time_ranges': ("2013-01-01", "2013-12-31"),
	'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
	'minValid':1,
	'normalized':True,
	'ndvi_threshold': 0.7,
	'vegetation_rate': 0.3,
	'slice_size': 3,
	'products': ["LS8_OLI_LASRC", "LS7_ETM_LEDAPS"],
	'mosaic': False
}

_queues = {

    'mascara-landsat': queue_utils.assign_queue(),
    'joiner-reduce': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'], unidades=len(_params['products'])),
    'compuesto-temporal-medianas-wf':queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'], unidades=len(_params['products']) ),
    'ndvi-wf' : queue_utils.assign_queue(),
	'bosque-no-bosque-wf' : queue_utils.assign_queue(),
    'joiner': queue_utils.assign_queue(input_type='multi_area',lat=_params['lat'], lon=_params['lon'] ),
    'test-reduce': queue_utils.assign_queue(),
}

args={
	'owner':'cubo',
	'start_date':airflow.utils.dates.days_ago(2),
	'execID':"bosqueNoBosque",
	'product':"LS8_OLI_LASRC"
}
dag=DAG(
	dag_id='bosque_no_bosque', default_args=args,
	schedule_interval=None,
	dagrun_timeout=timedelta(minutes=20)
)


masked0=dag_utils.queryMapByTile(
	lat=_params['lat'],
	lon=_params['lon'],
	time_ranges= _params['time_ranges'],
	algorithm="mascara-landsat", version="1.0",
	product=_params['products'][0],
	params={'bands':_params['bands']},
	queue=_queues['mascara-landsat'], dag=dag, taxprefix="masked_{}_".format(_params['products'][0])

)
if len(_params['products']) > 1:
	masked1 = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
									   time_ranges=_params['time_ranges'],
									   algorithm="mascara-landsat", version="1.0",
									   product=_params['products'][1],
									   params={'bands': _params['bands']},
									   queue=_queues['mascara-landsat'], dag=dag,  taxprefix="masked_{}_".format(_params['products'][1])

									   )
	full_query = dag_utils.reduceByTile(masked0 + masked1, algorithm="joiner-reduce", version="1.0", queue=_queues['joiner-reduce'], dag=dag,  taxprefix="joined" , params={'bands': _params['bands']})
else:
	full_query = masked0

medians=dag_utils.IdentityMap(
	full_query,
	algorithm="compuesto-temporal-medianas-wf",
	version="1.0",
	taxprefix="medianas_",
	queue=_queues['compuesto-temporal-medianas-wf'],
	dag=dag,
	params={
		'normalized':_params['normalized'],
        'bands':_params['bands'],
        'minValid': _params['minValid']
	},
)
ndvi=dag_utils.IdentityMap(medians, algorithm="ndvi-wf", version="1.0", queue=_queues['ndvi-wf'], dag=dag,  taxprefix="ndvi")
bosque=dag_utils.IdentityMap(
	ndvi,
	algorithm="bosque-no-bosque-wf",
	version="1.0",
	params={
		'ndvi_threshold': _params['ndvi_threshold'],
		'vegetation_rate':_params['vegetation_rate'],
		'slice_size':_params['slice_size']
	},
	queue=_queues['bosque-no-bosque-wf'], dag=dag,  taxprefix="bosque",
)


if _params['mosaic']:
	task_id = 'mosaic'
	algorithm = 'joiner'
	queue = _queues['joiner']

else:
	task_id = 'print_context'
	algorithm = 'test-reduce'
	queue = _queues['test-reduce']

join = CDColReduceOperator(
	task_id=task_id,
	algorithm=algorithm,
	version='1.0',
	queue=queue, dag=dag
)
map(lambda b: b >> join, bosque)