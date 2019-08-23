#!/usr/bin/python3
# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator, CDColBashOperator
from cdcol_utils import other_utils
from cdcol_plugin.operators import common
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pprint import pprint
import logging

logging.basicConfig(
    format='%(levelname)s : %(asctime)s : %(message)s',
    level=logging.DEBUG
)

# To print loggin information in the console
logging.getLogger().addHandler(logging.StreamHandler())


def queryMapByTile(lat, lon, time_ranges, queue, dag, algorithm, version, params={}, to_tiff=False, task_id="med", alg_folder=common.ALGORITHMS_FOLDER,  **kwargs):
    return [CDColQueryOperator(
        algorithm=algorithm, version=version,
        lat=(LAT, LAT + 1),
        lon=(LON, LON + 1),
        time_ranges=time_ranges,
        params=params,
        queue=queue,
        to_tiff=to_tiff,
        alg_folder=alg_folder,
        dag=dag, task_id="{}{}{}".format(task_id, str(LAT), str(LON)), **kwargs) for LAT in range(*lat) for LON in
        range(*lon)]


#def queryMapByTileByYear(lat, lon, time_ranges, queue, dag, algorithm, version, params={}, to_tiff=False, task_id="med", alg_folder=common.ALGORITHMS_FOLDER, **kwargs):
#    logging.info("queryMapByTileByYear Latitude: {}, Longitude: {}, Time Range: {}".format(lat,lon,time_ranges))
#    tasks = [CDColQueryOperator(
#        algorithm=algorithm, version=version,
#        lat=(LAT, LAT + 1),
#        lon=(LON, LON + 1),
#        time_ranges=("01-01-" + str(T), "31-12-" + str(T)),
#        params=params,
#        queue=queue,
#        to_tiff=to_tiff,
#        alg_folder=alg_folder,
#        dag=dag, task_id="{}{}{}_{}".format(task_id, str(LAT), str(LON), "01-01-" + str(T) + "_31-12-" + str(T)),
#        **kwargs) for LAT in range(*lat) for LON in range(*lon) for T in
#        range(int(time_ranges[0].split('-')[2]), (int(time_ranges[1].split('-')[2])) + 1)]
#    return tasks


def queryMapByTileByYear(lat, lon, time_ranges, queue, dag, algorithm, version, params={}, to_tiff=False, task_id="med", alg_folder=common.ALGORITHMS_FOLDER, **kwargs):
    logging.info("queryMapByTileByYear Latitude: {}, Longitude: {}, Time Range: {}".format(lat,lon,time_ranges))

    lat_min, lat_max = lat
    lon_min, lon_max = lon

    # Time ranges is a tuple 
    time_min, time_max = time_ranges

    time_min = datetime.strptime(time_min, '%Y-%m-%d')
    time_max = datetime.strptime(time_max, '%Y-%m-%d')

    tasks = []

    # range exclude the last number, example range(1,5) = [1,2,3,4]
    for latitude in range(lat_min,lat_max):
        for longitude in range(lon_min,lon_max):
            for time_year in range(time_min.year,time_max.year + 1):

                time_range_min =  "{}-{}-{}".format(time_year,time_min.strftime('%m'),time_min.strftime('%d'))
                time_range_max =  "{}-{}-{}".format(time_year,time_max.strftime('%m'),time_max.strftime('%d'))

                logging.info("queryMapByTileByYear Time Range {} and {}".format(time_range_min,time_range_max))

                # Task id can not be more than 250 charecters
                task_ident = "{}{}{}_{}".format(task_id, latitude, longitude, time_range_min + "_" + time_range_max)

                logging.info("queryMapByTileByYear Task id len: {}-{}".format( len(task_id),task_id ) )

                task = CDColQueryOperator(
                    dag=dag,
                    task_id=task_ident,
                    queue=queue,

                    algorithm=algorithm, 
                    version=version,
                    lat=(latitude, latitude + 1),
                    lon=(longitude, longitude + 1),
                    time_ranges=(time_range_min, time_range_max),
                    to_tiff=to_tiff,
                    alg_folder=alg_folder,
                    **kwargs
                )

                tasks.append(task)

    return tasks




def queryMapByTileByMonths(lat, lon, time_ranges, queue, dag, algorithm, version, params={}, months=12, to_tiff=False, task_id="med", alg_folder=common.ALGORITHMS_FOLDER,
                           **kwargs):
    tasks = []

    for LAT in range(*lat):
        for LON in range(*lon):
            start = datetime.strptime(time_ranges[0], '%Y-%m-%d')
            end = datetime.strptime(time_ranges[1], '%Y-%m-%d')
            while start <= end:
                tasks.append(CDColQueryOperator(algorithm=algorithm,version=version,
                                                lat=(LAT, LAT + 1),lon=(LON, LON + 1),
                                                time_ranges=(start.strftime('%d-%m-%Y'), (
                                                            start + relativedelta(months=months - 1,
                                                                                  day=end.day)).strftime('%d-%m-%Y')),
                                                params=params,
                                                queue=queue,
                                                to_tiff=to_tiff,
                                                alg_folder=alg_folder,
                                                dag=dag, task_id="{}{}{}_{}_{}".format(task_id, str(LAT), str(LON),
                                                                                       start.strftime('%d-%m-%Y'), (
                                                                                                   start + relativedelta(
                                                                                               months=months - 1,
                                                                                               day=end.day)).strftime(
                            '%d-%m-%Y'), **kwargs)))
                start += relativedelta(months=months)

    return tasks


def IdentityMap(upstream, algorithm, version, queue, dag, task_id, delete_partial_results=False, to_tiff=False, params={},
                           **kwargs):
    i = 1
    tasks = []
    trans = str.maketrans({"(": None, ")": None, " ": None, ",": "_"})
    for prev in upstream:
        id=("{}_{}_{}".format(task_id, prev.lat, prev.lon)).translate(trans)
        _t = CDColFromFileOperator(algorithm=algorithm, version=version, queue=queue, dag=dag, lat=prev.lat,
                                   lon=prev.lon,
                                   task_id=id,
                                   to_tiff=to_tiff,
                                   params=params,**kwargs)
        if delete_partial_results:
            delete = PythonOperator(task_id="del_"+prev.task_id,
                                provide_context=True,
                                python_callable=other_utils.delete_partial_result,
                                queue='util',
                                op_kwargs={'algorithm': prev.algorithm, 'version':prev.version, 'execID': prev.execID, 'task_id':prev.task_id},
                                dag=dag)
            delete << _t
        i += 1
        prev >> _t

        tasks.append(_t)

    return tasks


def DeleteMap(upstream, dag):
    i = 1
    tasks = []
    for prev in upstream:
        _t = PythonOperator(task_id='delete_partial_results_'+str(i),
                            provide_context=True,
                            python_callable=other_utils.delete_partial_results,
                            queue='util',
                            op_kwargs={'algorithms': {}, 'execID': prev.execID},
                            dag=dag)
        i += 1
        prev >> _t
        tasks.append(_t)
    return tasks


def BashMap(upstream, algorithm, version, queue, dag, task_id, delete_partial_results=False, params={}):
    i = 1
    tasks = []
    trans = str.maketrans({"(": None, ")": None, " ": None, ",": "_"})
    for prev in upstream:
        _t = CDColBashOperator(algorithm=algorithm, version=version, queue=queue, dag=dag, lat=prev.lat, lon=prev.lon,
                               task_id=("{}_{}_{}".format(task_id, prev.lat, prev.lon)).translate(trans), params=params)
        if delete_partial_results:
            delete = PythonOperator(task_id="del_"+prev.task_id,
                                provide_context=True,
                                python_callable=other_utils.delete_partial_result,
                                queue='util',
                                op_kwargs={'algorithm': prev.algorithm, 'version':prev.version, 'execID': prev.execID, 'task_id':prev.task_id},
                                dag=dag)
            delete << _t
        i += 1
        prev >> _t

        tasks.append(_t)

    return tasks


def OneReduce(upstream, algorithm, version, queue, dag, task_id, delete_partial_results=False,params={}, to_tiff=False, trigger_rule=None, **kwargs):
    reduce = CDColReduceOperator(
        task_id="{}_{}_{}".format(task_id, "all", "all"),
        algorithm=algorithm,
        version=version,
        params=params,
        queue=queue,
        trigger_rule=trigger_rule,
        to_tiff=to_tiff,
        dag=dag, **kwargs)
    upstream >> reduce
    if delete_partial_results:
        tasks = []
        for prev in upstream:
            delete = PythonOperator(task_id="del_" + prev.task_id,
                                    provide_context=True,
                                    python_callable=other_utils.delete_partial_result,
                                    queue='util',
                                    op_kwargs={'algorithm': prev.algorithm, 'version': prev.version,
                                               'execID': prev.execID, 'task_id': prev.task_id},
                                    dag=dag)
            tasks.append(delete)
        tasks << reduce


    return [reduce]


def reduceByTile(upstream, algorithm, version, queue, dag, task_id, delete_partial_results=False, to_tiff=False, params={}, **kwargs):
    reducers = {}
    trans = str.maketrans({"(": None, ")": None, " ": None, ",": "_"})
    for prev in upstream:
        key = "{}_{}".format(prev.lat, prev.lon)
        if key not in reducers:
            reducers[key] = CDColReduceOperator(
                task_id="{}_{}".format(task_id, key.translate(trans)),
                algorithm=algorithm,
                version=version,
                params=params,
                lat=prev.lat,
                lon=prev.lon,
                queue=queue,
                to_tiff=to_tiff,
                dag=dag, **kwargs)
        prev >> reducers[key]
        if delete_partial_results:
            delete = PythonOperator(task_id="del_"+prev.task_id,
                                provide_context=True,
                                python_callable=other_utils.delete_partial_result,
                                queue='util',
                                op_kwargs={'algorithm': prev.algorithm, 'version':prev.version, 'execID': prev.execID, 'task_id':prev.task_id},
                                dag=dag)
            delete << reducers[key]
    return reducers.values()


def print_xcom(ds, **kwargs):
    # pprint(kwargs)
    task = kwargs['task']
    task_instance = kwargs['task_instance']
    upstream_tasks = task.get_direct_relatives(upstream=True)
    upstream_task_ids = [task.task_id for task in upstream_tasks]
    upstream_variable_values = task_instance.xcom_pull(task_ids=upstream_task_ids, key='return_value')
    pprint(upstream_variable_values)
    # print(ds)
