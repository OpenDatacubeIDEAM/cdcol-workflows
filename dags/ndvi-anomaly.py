# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator, PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils

from datetime import timedelta
from pprint import pprint

_params = {
    'lat': (4,6),
	'lon': (-74,-72),
	'time_ranges': [("2016-01-01", "2016-12-31"),("2017-01-01", "2017-12-31")],
    'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
    'minValid':1,
    'normalized':True,
    'products': ["LS8_OLI_LASRC"],
	'mosaic': False
}

_queues = {

    'mascara-landsat': queue_utils.assign_queue(input_type='multi_temporal', time_range=_params['time_ranges'][0]),
    'joiner-reduce': queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'][0], unidades=len(_params['products'])),
    'compuesto-temporal-medianas-wf':queue_utils.assign_queue(input_type='multi_temporal_unidad', time_range=_params['time_ranges'][0], unidades=len(_params['products']) ),
    'ndvi-wf' : queue_utils.assign_queue(),
    'joiner': queue_utils.assign_queue(input_type='multi_area',lat=_params['lat'], lon=_params['lon'] ),
    'test-reduce': queue_utils.assign_queue(),
}

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "ndvi-anomaly",
    'product': "LS8_OLI_LASRC"
}

dag = DAG(
    dag_id='ndvi-anomaly', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

consulta_baseline=dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
	time_ranges= _params['time_ranges'][0],
	algorithm="mascara-landsat", version="1.0",
        product=_params['products'][0],
        params={
                'normalized':_params['normalized'],
                'bands':_params['bands'],
                'minValid': _params['minValid']
        },queue=_queues['mascara-landsat'],dag=dag, taxprefix="consulta_baseline_{}_".format(_params['products'][0])

)
consulta_analysis=dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
	time_ranges= _params['time_ranges'][1],
	algorithm="mascara-landsat", version="1.0",
        product=_params['products'][0],
        params={
                'normalized':_params['normalized'],
                'bands':_params['bands'],
                'minValid': _params['minValid']
        },queue=_queues['mascara-landsat'],dag=dag, taxprefix="consulta_analysis_{}_".format(_params['products'][0])

)


medians_baseline = dag_utils.IdentityMap(
    consulta_baseline,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_baseline_",
    queue=_queues['compuesto-temporal-medianas-wf'],
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

medians_analysis = dag_utils.IdentityMap(
    consulta_analysis,
    algorithm="compuesto-temporal-medianas-wf",
    version="1.0",
    taxprefix="medianas_analysis_",
    queue=_queues['compuesto-temporal-medianas-wf'],
    dag=dag,
    params={
        'normalized': _params['normalized'],
        'bands': _params['bands'],
        'minValid': _params['minValid'],
    })

ndvi_baseline=dag_utils.IdentityMap(medians_baseline, algorithm="ndvi-wf", version="1.0", queue=_queues['ndvi-wf'], dag=dag, taxprefix="ndvi_baseline")
ndvi_analysis=dag_utils.IdentityMap(medians_analysis, algorithm="ndvi-wf", version="1.0", queue=_queues['ndvi-wf'], dag=dag, taxprefix="ndvi_analysis")

ndvi_anomaly=dag_utils.reduceByTile(ndvi_analysis+ndvi_baseline, algorithm="ndvi-anomaly", version="1.0", queue="airflow_medium", params={'ndvi_baseline_threshold_range' : (0.60, 0.90)  },dag=dag, taxprefix="ndvi_anomaly")


delete_partial_results = PythonOperator(task_id='delete_partial_results',
                                            provide_context=True,
                                            python_callable=other_utils.delete_partial_results,
                                            queue='airflow_small',
                                            op_kwargs={'algorithms': {
                                                'mascara-landsat': "1.0",
                                                'ndvi-wf': "1.0",
												 'compuesto-temporal-medianas-wf':"1.0",
                                            }, 'execID': args['execID']},
                                            dag=dag)

if _params['mosaic']:
	mosaic = dag_utils.OneReduce(ndvi_anomaly, algorithm="joiner", version="1.0", queue=_queues['joiner'], dag=dag,taxprefix="mosaic")
	map(lambda b: b >> delete_partial_results, mosaic)

else:
	map(lambda b: b >> delete_partial_results, ndvi_anomaly)