#!/usr/bin/python3
# coding=utf8
import datetime

scores = {
    'small':range(0,3) ,
    'medium':range(3,5),
    'large':range(5,8),
    'xlarge':range(8,18)
}

input_types = {
    'unidad':0,
    'multi_unidad':1,
    'multi_temporal':2,
    'multi_unidad_temporal':3
}

def get_queue_by_score(score):
    if score in scores['small']:
        return 'airflow_small'
    elif score in scores['medium']:
        return 'airflow_medium'
    elif score in scores['large']:
        return 'airflow_large'
    else:
        return 'airflow_xlarge'

def get_tiles(lat={}, lon={}):
    return (lat[1]-lat[0])*(lon[1]-lon[0])

def assign_queue(input_type={}, time_range={}, lat={}, lon={}, unidades={} ):

    score = 1
    if input_type:
        if 'temporal' in input_type:
            try:
                start_date_value = datetime.datetime.strptime(time_range[0], "%d-%m-%Y")
                end_date_value = datetime.datetime.strptime(time_range[1], "%d-%m-%Y")
            except:
                start_date_value = datetime.datetime.strptime(time_range[0], "%Y-%m-%d")
                end_date_value = datetime.datetime.strptime(time_range[1], "%Y-%m-%d")

            anhos = 1 + (end_date_value.year - start_date_value.year)
            score *= anhos
        if 'area' in input_type:
            tiles = get_tiles(lat, lon)
            score *= tiles
        if 'unidad' in input_type:
            score *= unidades
    return get_queue_by_score(score)

