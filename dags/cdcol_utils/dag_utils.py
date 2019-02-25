#!/usr/bin/python3
# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator, CDColBashOperator

from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pprint import pprint

def queryMapByTile(lat,lon,time_ranges, queue, dag,  algorithm,version,params={},taxprefix="med",**kwargs):
    return [CDColQueryOperator(
        algorithm=algorithm,version=version,
        lat=(LAT,LAT+1), 
        lon=(LON,LON+1),
        time_ranges=time_ranges,
        params=params,
        queue=queue,
        dag=dag, task_id="{}{}{}".format(taxprefix,str(LAT),str(LON)),**kwargs) for LAT in range(*lat) for LON in range(*lon)]

def queryMapByTileByYear(lat,lon,time_ranges,queue, dag, algorithm,version,params={},taxprefix="med",**kwargs):

    return [CDColQueryOperator(
        algorithm=algorithm, version=version,
        lat=(LAT, LAT + 1),
        lon=(LON, LON + 1),
        time_ranges=("01-01-"+str(T),"31-12-"+str(T)),
        params=params,
        queue=queue,
        dag=dag, task_id="{}{}{}_{}".format(taxprefix, str(LAT), str(LON), "01-01-"+str(T)+"_31-12-"+str(T)), **kwargs) for LAT in range(*lat) for LON in range(*lon) for T in xrange(int(time_ranges[0].split('-')[0]), (int(time_ranges[1].split('-')[0]))+1) ]

def queryMapByTileByMonths(lat,lon,time_ranges,queue, dag, algorithm,version,params={},months=12,taxprefix="med",  **kwargs):
    tasks = []
    
    for LAT in range(*lat):
        for LON in range(*lon):
            start = datetime.strptime(time_ranges[0],'%Y-%m-%d')
            end = datetime.strptime(time_ranges[1],'%Y-%m-%d')
            while start <= end:
                tasks.append(CDColQueryOperator(algorithm=algorithm,
                                                version=version,
                                                lat=(LAT, LAT + 1),
                                                lon=(LON, LON + 1),
                                                time_ranges=(start.strftime('%d-%m-%Y'), (start + relativedelta(months=months-1, day=end.day)).strftime('%d-%m-%Y')),
                                                params=params,
                                                queue=queue,
                                                dag=dag, task_id="{}{}{}_{}_{}".format(taxprefix, str(LAT), str(LON), start.strftime('%d-%m-%Y') , (start + relativedelta(months=months-1, day=end.day)).strftime('%d-%m-%Y'),**kwargs)))
                start += relativedelta(months=months)

    return tasks

def IdentityMap(upstream,algorithm,version, queue, dag, taxprefix,params={}):
    i=1
    tasks=[]
    trans = str.maketrans({"(": None, ")": None, " ": None, ",": "_"})
    for prev in upstream:
        _t=CDColFromFileOperator(algorithm=algorithm,version=version,queue=queue, dag=dag,  lat=prev.lat, lon=prev.lon, task_id=("{}_{}_{}".format(taxprefix,prev.lat,prev.lon)).translate(trans),params=params)
        i+=1
        prev>>_t
        tasks.append(_t)

    return tasks

def BashMap(upstream,algorithm,version, queue, dag, task_id ,params={}):
    i = 1
    tasks = []
    trans = str.maketrans({"(": None, ")": None, " ": None, ",": "_"})
    for prev in upstream:
        _t = CDColBashOperator(algorithm=algorithm, version=version, queue=queue, dag=dag, lat=prev.lat, lon=prev.lon, task_id=("{}_{}_{}".format(task_id,prev.lat,prev.lon)).translate(trans), params=params)
        i += 1
        prev >> _t
        tasks.append(_t)

    return tasks
    
def OneReduce(upstream, algorithm,version, queue, dag,  taxprefix, params={}):
    reduce= CDColReduceOperator(
        task_id="{}_{}_{}".format(taxprefix,"all","all" ),
        algorithm=algorithm,
        version=version,
        params=params,
        queue=queue,
        dag=dag)
    upstream>>reduce
    return [reduce]
    
def reduceByTile(upstream, algorithm,version, queue, dag,  taxprefix, params={}):
    reducers={}
    trans = str.maketrans({"(": None, ")": None, " ": None, ",": "_"})
    for prev in upstream:
        key="{}_{}".format(prev.lat,prev.lon)
        if key not in reducers:
            reducers[key]=CDColReduceOperator(
                task_id=taxprefix+key.translate(trans),
                algorithm=algorithm,
                version=version,
                params=params,
                lat=prev.lat,
                lon=prev.lon,
                queue=queue,
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

