import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import CDColQueryOperator, CDColFromFileOperator


from datetime import timedelta
from pprint import pprint
def queryMapByTile(lat,lon,time_ranges,dag, algorithm,version,params={},taxprefix="med"):
    return [CDColQueryOperator(
        algorithm="compuesto-temporal-de-medianas",version="1.0",
        
        lat=(LAT,LAT+1), 
        lon=(LON,LON+1),
        time_ranges=time_ranges,
        params=params,
        dag=dag, task_id="{}{}{}".format(taxprefix,str(LAT),str(LON))) for LAT in range(*lat) for LON in range(*lon)]
        
      
def IdentityMap(upstream,algorithm,version,dag, taxprefix,params={}):
    i=1
    tasks=[]
    for prev in upstream:
        _t=CDColFromFileOperator(algorithm=algorithm,version=version,dag=dag, task_id=taxprefix+str(i),params=params)
        i+=1
        prev>>_t
        tasks.append(_t)
    
    return tasks
    
    
def print_xcom(ds, **kwargs):
    #pprint(kwargs)
    task=kwargs['task']
    task_instance = kwargs['task_instance']
    upstream_tasks = task.get_direct_relatives(upstream=True)
    upstream_task_ids = [task.task_id for task in upstream_tasks]
    upstream_variable_values = task_instance.xcom_pull(task_ids=upstream_task_ids, key='return_value')
    pprint(upstream_variable_values)
    #print(ds)
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':"bosqueMultitile",
    'product':"LS8_OLI_LASRC",
}

dag = DAG(
    dag_id='bosque-no-bosqueFull', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15))
    
mediansComposites = queryMapByTile( lat=(2,4), 
        lon=(-69,-67),time_ranges=[("2013-01-01","2013-12-31")],
        algorithm="compuesto-temporal-de-medianas",version="1.0", 
        params={
            'normalized':True,
            'bands':["blue","green","red","nir", "swir1","swir2"], 
            'minValid':1,
        },
        dag=dag, taxprefix="med")

ndvis = IdentityMap(mediansComposites, algorithm="ndvi-wf",version="1.0",dag=dag, taxprefix="ndvi")
bosque = IdentityMap(ndvis,algorithm="bosque-no-bosque-wf",version="1.0",
        params={
            'ndvi_threshold':0.7,
            'vegetation_rate':0.3,
            'slice_size':3
        },
        dag=dag, taxprefix="bosque")

reduce= PythonOperator(
    task_id='print_context',
    provide_context=True,
    python_callable=print_xcom,
    dag=dag)

map(lambda b: b>>reduce,bosque)