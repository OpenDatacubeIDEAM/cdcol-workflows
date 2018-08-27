_lat=(2,4)
_lon=(-69,-67)

args = {
    'owner': 'cubo',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"queryByYearTest",
    'product':"queryByYear"
}

dag = DAG(
    dag_id='query_by_year', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15))

maskedLS8 = dag_utils.queryMapByTileByYear( lat=_lat,
        lon=_lon,time_ranges=[("2013-01-01","2014-12-31")],
        algorithm="mascara-landsat",version="1.0",
        product="LS8_OLI_LASRC",
        params={
            'normalized':True,
            'bands':["blue","green","red","nir", "swir1","swir2"],
            'minValid':1,
        },
        dag=dag, taxprefix="maskedLS8_")

reduce= CDColReduceOperator(
    task_id='print_context',
    algorithm='test-reduce',
    version='1.0',
    dag=dag)

map(lambda b: b>>reduce,maskedLS8)