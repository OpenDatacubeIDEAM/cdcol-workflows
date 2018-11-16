# coding=utf-8

from datetime import timedelta
import airflow
import uuid
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from cdcol_utils import dag_utils
import json

'''Script para generar el DAG para el algoritmo compuesto de medianas a partir de un archivo Json '''

with open('/home/cubo/airflow/JsonScript/dags/JsonMedianasEjemplo.json') as f:
    data = json.load(f)

'''Se definen los argumentos genéricos a partir del archivo Json pasado.'''
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': data['workflow']+str(data['Version'])+str(uuid.uuid4()),
    'product': data['Product']
}

dag = DAG(
    dag_id='compuesto_de_medianas_Json_test'+str(uuid.uuid4()), default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=15))

#Se definen las tuplas con las posibles bandas para cada version de landsat
LS7_bands = ('blue', 'green', 'red', 'nir', 'swir1', 'swir2')

LS8_bands = ('ultra_blue', 'blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'panchromatic', 'cirrus', 'tirs1', 'tirs2')

#Define las bandas que se especifican en el Json para la versión 7 de landsat.
LS7_JsonBands = [val for val in data['Bands'] if val in LS7_bands]

LS8_JsonBands = [val for val in data['Bands'] if val in LS8_bands]


#Se genera la tarea maskedLS8 sólo si en el archivo Json especifican LS8 como el producto o uno de los productos
if 'LS8' in data['Product']:
    maskedLS8 = dag_utils.queryMapByTile(lat=data['Lat'],
                                         lon=data['Lon'], time_ranges=[(data['Time_range'][0]), data['Time_range'][1]],
                                         algorithm="mascara-landsat", version="1.0",
                                         product=data['Product'],
                                         params={
                                             'normalized':  data['Normalized'],
                                             'bands': LS8_JsonBands,
                                             'minValid': data['Min_valid'],
                                         },
                                         dag=dag, taxprefix="maskedLS8_")

if 'LS7' in data['Product']:
    maskedLS7 = dag_utils.queryMapByTile(lat=data['Lat'],
                                         lon=data['Lon'], time_ranges=[(data['Time_range'][0]), data['Time_range'][1]],
                                         algorithm="mascara-landsat", version="1.0",
                                         product=data['Product'],
                                         params={
                                             'normalized':  data['Normalized'],
                                             'bands': LS7_JsonBands,
                                             'minValid': data['Min_valid'],
                                         },
                                         dag=dag, taxprefix="maskedLS7_")

if 'LS8' in data['Product']:
    LS8_medians = dag_utils.IdentityMap(
        maskedLS8,
        algorithm="compuesto-temporal-medianas-wf",
        version="1.0",
        taxprefix="medianas_",
        dag=dag,
        params={
            'normalized': data['Product'],
            'bands': LS8_JsonBands,
            'minValid': data['Min_valid'],
        })

if 'LS7' in data['Product']:
    LS7_medians = dag_utils.IdentityMap(
        maskedLS7,
        algorithm="compuesto-temporal-medianas-wf",
        version="1.0",
        taxprefix="medianas_",
        dag=dag,
        params={
            'normalized': data['Product'],
            'bands': LS8_JsonBands,
            'minValid': data['Min_valid'],
        })

mosaic = CDColReduceOperator(
    task_id='print_context',
    algorithm='joiner',
    version='1.0',
    dag=dag)

list_map = [l for l in [LS7_medians, LS8_medians] if l is not None]

map(lambda b: b >> mosaic, list_map)








