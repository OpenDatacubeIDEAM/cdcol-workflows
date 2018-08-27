# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator


from datetime import timedelta, datetime
from pprint import pprint

def queryMapByTile(lat,lon,time_ranges,dag, algorithm,version,params={},taxprefix="med",**kwargs):
    return [CDColQueryOperator(
        algorithm=algorithm,version=version,
        lat=(LAT,LAT+1), 
        lon=(LON,LON+1),
        time_ranges=time_ranges,
        params=params,
        dag=dag, task_id="{}{}{}".format(taxprefix,str(LAT),str(LON)),**kwargs) for LAT in range(*lat) for LON in range(*lon)]

def queryMapByTileByYear(lat,lon,time_ranges,dag, algorithm,version,params={},taxprefix="med",**kwargs):

    return [CDColQueryOperator(
        algorithm=algorithm, version=version,

        lat=(LAT, LAT + 1),
        lon=(LON, LON + 1),
        time_ranges=[("01-01-"+str(T),+"31-12-"+str(T))],
        params=params,
        dag=dag, task_id="{}{}{}{}".format(taxprefix, str(LAT), str(LON), str("01-01-"+T+"_31-12-"+T)), **kwargs) for LAT in range(*lat) for LON in range(*lon)
        for T in xrange(int(time_ranges[0][0].split('-')[2]), int(time_ranges[0][1].split('-')[2]) + 1) ]


def IdentityMap(upstream,algorithm,version,dag, taxprefix,params={}):
    i=1
    tasks=[]
    for prev in upstream:
        _t=CDColFromFileOperator(algorithm=algorithm,version=version,dag=dag,lat=prev.lat, lon=prev.lon, task_id=taxprefix+str(i),params=params)
        i+=1
        prev>>_t
        tasks.append(_t)
    
    return tasks
    
def OneReduce(upstream, algorithm,version, dag, tax_id, params={}):
    reduce= CDColReduceOperator(
        task_id=task_id,
        algorithm=algorithm,
        version=version,
        params=params,
        dag=dag)
    map(lambda b: b>>reduce,upstream)
    return reduce
    
def reduceByTile(upstream, algorithm,version, dag, taxprefix, params={}):
    reducers={}
    for prev in upstream:
        key="{}_{}".format(prev.lat,prev.lon)
        if key not in reducers:
            reducers[key]=CDColReduceOperator(
                task_id=taxprefix+key.translate(None,"() ").replace(",","-"),
                algorithm=algorithm,
                version=version,
                params=params,
                lat=prev.lat,
                lon=prev.lon,
                dag=dag)
        prev>>reducers[key]
    return reducers.values()
    
def print_xcom(ds, **kwargs):
    #pprint(kwargs)
    task=kwargs['task']
    task_instance = kwargs['task_instance']
    upstream_tasks = task.get_direct_relatives(upstream=True)
    upstream_task_ids = [task.task_id for task in upstream_tasks]
    upstream_variable_values = task_instance.xcom_pull(task_ids=upstream_task_ids, key='return_value')
    pprint(upstream_variable_values)
    #print(ds)
