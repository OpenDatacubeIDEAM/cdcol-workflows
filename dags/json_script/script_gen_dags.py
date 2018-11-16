# coding=utf-8

import json
import subprocess
import sys
from datetime import datetime
import time
from pytz import timezone

from jinja2 import Environment, FileSystemLoader

'''Script para generar el DAG para el algoritmo compuesto de medianas a partir de un archivo Json '''

airflow_path = '/home/cubo/airflow/'
templates_path = '/home/cubo/workflows/dags/json_script/templates_dag/'
json_abs_path = sys.argv[1]


def ascii_encode_dict(data):
    ascii_encode = lambda x: x.encode('ascii', 'ignore') if isinstance(x, unicode) else x
    return dict(map(ascii_encode, pair) for pair in data.items())


def ascii_encode_list(data):
    return [x.encode('ascii', 'ignore') if isinstance(x, unicode) else x for x in data]


with open(json_abs_path, 'r') as f:
    data = json.loads(f.read(), object_hook=ascii_encode_dict)

_params = {}


def params_generation(data):
    if 'lat' in data:
        _params['lat'] = data['lat']
    if 'lat' in data:
        _params['lat'] = data['lat']
    if 'lon' in data:
        _params['lon'] = data['lon']
    if 'time_ranges' in data:
        _params['time_ranges'] = [(data['time_ranges'][0]).encode('ascii', 'ignore'),
                                 (data['time_ranges'][1]).encode('ascii', 'ignore')]
    if 'bands' in data:
        _params['bands'] = ascii_encode_list(data['bands'])
    if 'minValid' in data:
        _params['minValid'] = data['minValid']
    if 'normalized' in data:
        _params['normalized'] = data['normalized']
    if 'version' in data:
        _params['version'] = data['version']
    if 'owner' in data:
        _params['owner'] = data['owner']
    if 'products' in data:
        _params['products'] = data['products']
    if 'ndvi_threshold' in data:
        _params['ndvi_threshold'] = data['ndvi_threshold']
    if 'slice_size' in data:
        _params['slice_size'] = data['slice_size']
    if 'vegetation_rate' in data:
        _params['vegetation_rate'] = data['vegetation_rate']
    if 'mosaic' in data:
        _params['mosaic'] = data['mosaic']
    if 'modelos' in data:
        _params['modelos'] = data['modelos']
    if 'classes' in data:
        _params['classes'] = data['classes']
    _params['execID'] = data['workflow']+str('-'+data['version']+'-'+data['owner']+'-')+now.strftime('%Y-%m-%d_%H-%M-%S')


wf_name = data['workflow']

tz = timezone('America/Bogota')

now = datetime.now(tz)

'''Parámetros provenientes del Json'''
params_generation(data)

file_dict = {
    'compuesto_medianas': 'compuesto_medianas.txt',
    'bosque_no_bosque': 'bosque_no_bosque.txt',
    'ndsi': 'ndsiTXT.txt',
    'ndvi': 'ndvi.txt',
    'wofs': 'wofs.txt',
    'k_means': 'k_means.txt',
    'deteccion_de_cambios_pca': 'deteccion_de_cambios_pca.txt',
    'clasificador_generico': 'clasificador_generico.txt'
}

with open(
        str(airflow_path) + 'dags/Generado-' + str(_params['execID']) + '.py', 'a') as wf:
    file_loader = FileSystemLoader(str(templates_path))
    env = Environment(loader=file_loader)
    template = env.get_template(file_dict[wf_name])
    output = template.render(params=_params)
    wf.write(output)


bash_command1 = 'airflow list_dags'

subprocess.call(bash_command1.split())

