import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from airflow.operators.python_operator import PythonOperator
from cdcol_utils import dag_utils, queue_utils, other_utils
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta
from pprint import pprint

_params = {{params}}

# definir unidades FNF, y DEM por defecto (una sola banda)
_params['products'].append({'name': 'DEM_Mosaico', 'bands': ['dem']})
_params['products'].append({'name': 'FNF_COL_UTM', 'bands': ['fnf_mask']})

# Definir periodo de tiempo DEM
_params['time_ranges'] = [('2013-01-01','2013-12-31')] + _params['time_ranges']

# sort params products by name
_params['products'].sort(key = lambda d: d['name'])

"""
Templeate modified my Crhsitian Segura
27-oct-2020
"""
"""
_params = {'minValid': 1, 'normalized': False,
    'modelos': '/web_storage/downloads/3625',
    'lat': (10, 11),
    'lon': (-75, -74),
    'bands': ["red", "nir", "swir1", "swir2"],
    #'minValid': 1,
    'products': [{'name': 'LS7_ETM_LEDAPS_MOSAIC', 'bands': ['swir2', 'nir', 'red', 'swir1']},{'name': 'DEM_Mosaico', 'bands': ['dem']},{'name': 'FNF_COL_UTM', 'bands': ['fnf_mask']}],
    'time_ranges': [('2016-01-01', '2016-12-31'),('2013-01-01', '2013-12-31'),('2017-01-01', '2017-12-31')],
    'execID': "ctm_b_01",
    'elimina_resultados_anteriores': True,
    'genera_mosaico': True,
    #'genera_geotiff': True,
    'owner': 'cubo',
    'normalized': False
}
"""
_steps = {
    'mascara': {
        'algorithm': "mascara-landsat",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal', time_range=_params['time_ranges'][2]),
        'params': {'bands': _params['products'][2]['bands']},
    },
    'consulta': {
        'algorithm': "mascara-landsat",
        'version': '2.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_temporal_area',
            time_range=_params['time_ranges'][2],
            lat=_params['lat'], lon=_params['lon']),
        'params': {'bands': _params['products'][2]['bands']},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'reduccion': {
        'algorithm': "joiner",
        'version': '1.0',
        'queue': 'airflow_xlarge',
        'params': {'bands': _params['products'][2]['bands']},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'medianas': {
        'algorithm': "compuesto-temporal-medianas-indices-wf",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal_unidad',
                                          time_range=_params['time_ranges'][2],
                                          unidades=len(_params['products'])),
        'params': {
            'bands': _params['products'][2]['bands'],
            'minValid': _params['minValid'],
            'normalized': False,
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'medianas_dem': {
        'algorithm': "compuesto-temporal-medianas-indices-wf",
        'version': '3.0',
        'queue': queue_utils.assign_queue(input_type='multi_temporal_unidad',
                                          time_range=_params['time_ranges'][2],
                                          unidades=len(_params['products'])),
        'params': {
            'bands': _params['products'][2]['bands'],
            'minValid': _params['minValid'],
            'normalized': False,
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'mosaico': {
        'algorithm': "joiner",
        'version': '1.0',
        'queue': 'airflow_xlarge',
        'params': {},
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'entrenamiento': {
        'algorithm': "ensemble-training",
        'version': '3.0',
        'queue': queue_utils.assign_queue(
            input_type='multi_area',
            lat=_params['lat'],
            lon=_params['lon']
        ),
        'params': {
            'bands': _params['products'][2]['bands'],
            'train_data_path': _params['modelos']
        },
        'del_prev_result': False,
    },
    'clasificador': {
        'algorithm': "clasificador-ensemble-wf",
        'version': '2.0',
        'queue': 'airflow_xlarge',
        'params': {
            'bands': _params['products'][0]['bands'],
            'modelos': _params['modelos']
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
     'mascara_fnf': {
        'algorithm': "mascara_fnf",
        'version': '1.0',
        'queue': 'airflow_medium',
        'params': {
            'bands': _params['products'][1]['bands'],
            'modelos': _params['modelos']
        },
        'del_prev_result': _params['elimina_resultados_anteriores'],
    },
    'geotiff': {
        'algorithm': "generate-geotiff",
        'version': '1.0',
        'queue': queue_utils.assign_queue(input_type='multi_area', lat=_params['lat'], lon=_params['lon']),
        'params': {},
        'del_prev_result': False,
    }

}

args = {
    'owner': _params['owner'],
    'start_date': airflow.utils.dates.days_ago(2),
    'execID':_params['execID'],
    'product': _params['products'][2]
}

dag = DAG(
    dag_id=args['execID'], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))





mascara_0 = dag_utils.queryMapByTile(
    lat=_params['lat'], 
    lon=_params['lon'],
    time_ranges=_params['time_ranges'][2],
    algorithm=_steps['mascara']['algorithm'],
    version=_steps['mascara']['version'],
    product=_params['products'][2],
    params=_steps['mascara']['params'],
    queue=_steps['mascara']['queue'],
    dag=dag,
    task_id="mascara_" + _params['products'][2]['name']
)

if len(_params['products']) > 3:
    mascara_1 = dag_utils.queryMapByTile(
        lat=_params['lat'],
        lon=_params['lon'],
        time_ranges=_params['time_ranges'][2],
        algorithm=_steps['mascara']['algorithm'],
        version=_steps['mascara']['version'],
        product=_params['products'][3],
        params=_steps['mascara']['params'],
        queue=_steps['mascara']['queue'],
        dag=dag,
        task_id="mascara_" + _params['products'][3]['name']
    )

    reduccion_lansat = dag_utils.reduceByTile(
        mascara_0 + mascara_1,
        algorithm=_steps['reduccion']['algorithm'],
        version=_steps['reduccion']['version'],
        queue=_steps['reduccion']['queue'],
        product=_params['products'][2],
        dag=dag, task_id="joined",
        delete_partial_results=_steps['reduccion']['del_prev_result'],
        params=_steps['reduccion']['params'], 
    )
else:
    reduccion_lansat = mascara_0


medianas = dag_utils.IdentityMap(
                                reduccion_lansat,
                                product=_params['products'][2],
                                algorithm=_steps['medianas']['algorithm'],
                                version=_steps['medianas']['version'],
                                task_id="medianas",
                                queue=_steps['medianas']['queue'],
                                dag=dag,
                                delete_partial_results=_steps['medianas']['del_prev_result'],
                                params=_steps['medianas']['params']
)


mascara_dem_mosaic = dag_utils.queryMapByTile(lat=_params['lat'], lon=_params['lon'],
                                       time_ranges=_params['time_ranges'][0],
                                       algorithm=_steps['consulta']['algorithm'],
                                       version=_steps['consulta']['version'],
                                       product=_params['products'][0],
                                       params=_steps['consulta']['params'],
                                       queue=_steps['consulta']['queue'], dag=dag,
				       delete_partial_results=_steps['consulta']['del_prev_result'],
                                       task_id="consulta_referencia_" + _params['products'][0]['name'])


mascara_fnf_mosaic = CDColQueryOperator(lat=_params['lat'], lon=_params['lon'],
                                        time_ranges=_params['time_ranges'][1],
                                        algorithm=_steps['consulta']['algorithm'],
                                        version=_steps['consulta']['version'],
                                        product=_params['products'][1],
                                        params=_steps['consulta']['params'],
                                        queue=_steps['consulta']['queue'],
				        delete_partial_results=_steps['consulta']['del_prev_result'],
                                        dag=dag,
                                        task_id="consulta_referencia_" + _params['products'][1]['name'])

reduccion = dag_utils.reduceByTile(
                                    medianas + mascara_dem_mosaic,
                                    product=_params['products'][2],
                                    algorithm=_steps['reduccion']['algorithm'],
                                    version=_steps['reduccion']['version'],
                                    queue=_steps['reduccion']['queue'],
                                    dag=dag, task_id="joined_2",
                                    delete_partial_results=_steps['reduccion']['del_prev_result'],
                                    params=_steps['reduccion']['params'],
)


medianas_dem = dag_utils.IdentityMap(
                                reduccion,
                                product=_params['products'][2],
                                algorithm=_steps['medianas_dem']['algorithm'],
                                version=_steps['medianas_dem']['version'],
                                task_id="medianas_dem",
                                queue=_steps['medianas_dem']['queue'],
                                dag=dag,
                                delete_partial_results=_steps['medianas_dem']['del_prev_result'],
                                params=_steps['medianas_dem']['params']
)




mosaico = dag_utils.OneReduce(medianas_dem, task_id="mosaico_consulta",
                              algorithm=_steps['mosaico']['algorithm'],
                              version=_steps['mosaico']['version'],
                              queue=_steps['mosaico']['queue'],
                              delete_partial_results=_steps['mosaico']['del_prev_result'],
                              trigger_rule=TriggerRule.NONE_FAILED, dag=dag)

entrenamiento = dag_utils.IdentityMap(
                                mosaico,
                                algorithm=_steps['entrenamiento']['algorithm'],
                                version=_steps['entrenamiento']['version'],
                                task_id="entrenamiento",
                                queue=_steps['entrenamiento']['queue'],
                                dag=dag,
                                delete_partial_results=_steps['entrenamiento']['del_prev_result'],
                                params=_steps['entrenamiento']['params']
)

clasificador = CDColReduceOperator(
                                task_id="clasificador_generico",
                                algorithm=_steps['clasificador']['algorithm'],
                                version=_steps['clasificador']['version'],
                                queue=_steps['clasificador']['queue'],
                                dag=dag,
                                lat=_params['lat'],
                                lon=_params['lon'],
                                params=_steps['clasificador']['params'],
                                delete_partial_results=_steps['clasificador']['del_prev_result'],

)

mascara_fnf = CDColReduceOperator(algorithm=_steps['mascara_fnf']['algorithm'],
                                       version=_steps['mascara_fnf']['version'],
                                       queue=_steps['mascara_fnf']['queue'],
                                       params=_steps['mascara_fnf']['params'],
                                       delete_partial_results=_steps['mascara_fnf']['del_prev_result'],
                                       dag=dag, task_id="clasificacion_final", to_tiff=True)


entrenamiento>>clasificador
mosaico>>clasificador



clasificador>>mascara_fnf
mascara_fnf_mosaic>>mascara_fnf

