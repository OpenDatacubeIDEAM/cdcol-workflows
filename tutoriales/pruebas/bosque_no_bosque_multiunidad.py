#coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from dags.cdcol_utils import dag_utils
from datetime import timedelta
from pprint import pprint 

_params = {
	'lat': (0,2),
	'lon': (-74,-72),
	'time_ranges': [("2014-01-01", "2014-12-31")],
	'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
	'minValid':1,
	'normalized':True,
	'ndvi_threshold': 0.7,
	'vegetation_rate': 0.3,
	'slice_size': 3
}

args={
	'owner':'cubo',
	'start_date':airflow.utils.dates.days_ago(2),
	'execID':"bosqueNoBosqueMultiUnidad",
	'product':"Multiple"
}
dag=DAG(
	dag_id='bosque_no_bosque_multiunidad', default_args=args,
	schedule_interval=None, 
	dagrun_timeout=timedelta(minutes=120)
)
maskedLS8 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'],
                                     time_ranges=_params['time_ranges'],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS8_OLI_LASRC",
                                     params={
                                         'normalized':_params['normalized'],
                                         'bands': _params['bands'],
                                         'minValid': _params['minValid'],
                                     },
                                     dag=dag, taxprefix="maskedLS8_")

maskedLS7 = dag_utils.queryMapByTile(lat=_params['lat'],
                                     lon=_params['lon'],
                                     time_ranges=_params['time_ranges'],
                                     algorithm="mascara-landsat", version="1.0",
                                     product="LS7_ETM_LEDAPS",
                                     params={
                                         'normalized':_params['normalized'],
                                         'bands': _params['bands'],
                                         'minValid': _params['minValid'],
                                     },
                                     dag=dag, taxprefix="maskedLS7_")
joins=dag_utils.reduceByTile(maskedLS7+maskedLS8, algorithm="joiner-reduce", version="1.0", dag=dag, taxprefix="joined")
medians=dag_utils.IdentityMap(
	joins,
	algorithm="compuesto-temporal-medianas-wf",
	version="1.0",
	taxprefix="medianas_",
	dag=dag,
	params={
		'normalized': _params['normalized'],
		'bands': _params['bands'],
		'minValid': _params['minValid']
	},
)
ndvi=dag_utils.IdentityMap(medians, algorithm="ndvi-wf", version="1.0", dag=dag, taxprefix="ndvi")
bosque=dag_utils.IdentityMap(
	ndvi, 
	algorithm="bosque-no-bosque-wf", 
	version="1.0",
	params={
		'ndvi_threshold': _params['ndvi_threshold'],
		'vegetation_rate': _params['vegetation_rate'],
		'slice_size': _params['slice_size']
	},
	dag=dag, taxprefix="bosque",
)
mosaic=CDColReduceOperator(
	task_id='print_context',
	algorithm='joiner',
	version='1.0',
	dag=dag
)
map(lambda b: b>>mosaic,bosque)
