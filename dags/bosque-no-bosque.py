#coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils
from datetime import timedelta
from pprint import pprint

_lat=(2,4)
_lon=(-69,-67)

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
maskedLS8=dag_utils.queryMapByTile(lat=_lat, lon=_lon,
	time_ranges=[("2013-01-01", "2013-12-31")],
	algorithm="mascara-landsat", version="1.0",
        product="LS8_OLI_LASRC",
        params={
                'normalized':True,
                'bands':["blue", "green", "red", "nir", "swir1", "swir2"],
                'minValid':1
        },
        dag=dag, taxprefix="maskedLS8_"

)

medians=dag_utils.IdentityMap(
	maskedLS8,
	algorithm="compuesto-temporal-medianas-wf",
	version="1.0",
	taxprefix="medianas_",
	dag=dag,
	params={
		'normalized':True,
		'bands':["blue", "green", "red", "nir", "swir1", "swir2"],
		'minValid':1
	},
)
ndvi=dag_utils.IdentityMap(medians, algorithm="ndvi-wf", version="1.0", dag=dag, taxprefix="ndvi")
bosque=dag_utils.IdentityMap(
	ndvi,
	algorithm="bosque-no-bosque-wf",
	version="1.0",
	params={
		'ndvi_threshold':0.7,
		'vegetation_rate':0.3,
		'slice_size':3
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
