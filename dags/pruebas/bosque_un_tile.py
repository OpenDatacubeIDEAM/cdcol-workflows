import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import CDColQueryOperator, CDColFromFileOperator

from datetime import timedelta
 
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"bosqueUnTile",
    'product':"LS8_OLI_LASRC",
}

dag = DAG(
    dag_id='bosque-no-bosque', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15))
    
mediansComposite = CDColQueryOperator(
        algorithm="compuesto-temporal-de-medianas",version="1.0",
        
        lat=(2,3), 
        lon=(-69,-68),
        time_ranges=[("2013-01-01","2013-12-31")],
        params={
            'normalized':True,
            'bands':["blue","green","red","nir", "swir1","swir2"], 
            'minValid':1,
        },
        dag=dag, task_id="med1")

ndvi = CDColFromFileOperator(algorithm="ndvi-wf",version="1.0",dag=dag, task_id="ndvi1")
bosque = CDColFromFileOperator(algorithm="bosque-no-bosque-wf",version="1.0",
        params={
            'ndvi_threshold':0.7,
            'vegetation_rate':0.3,
            'slice_size':3
        },
        dag=dag, task_id="bosque1")
mediansComposite>>ndvi>>bosque