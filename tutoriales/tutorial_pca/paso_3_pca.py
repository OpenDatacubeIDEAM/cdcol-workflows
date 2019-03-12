# coding=utf8
import airflow
from airflow.models import DAG
from airflow.operators import CDColQueryOperator, CDColFromFileOperator, CDColReduceOperator
from datetime import timedelta
from pprint import pprint

args = {
    'owner': 'mp.mancipe10',
    'start_date': airflow.utils.dates.days_ago(2),
    'execID': "mp.mancipe10_pca_paso_3_pca",
    'product': "LS7_ETM_LEDAPS"
}

dag = DAG(
    dag_id=args['execID'], default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=120))

consulta_2013 = CDColQueryOperator(algorithm="mascara-landsat",
                                   version="1.0",
                                   lat=(10, 11),
                                   lon=(-75, -74),
                                   product="LS7_ETM_LEDAPS",
                                   time_ranges=("2013-01-01", "2013-12-31"),
                                   params={
                                       'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
                                   },
                                   queue='airflow_small', dag=dag, task_id="consulta_p_2013")
consulta_2014 = CDColQueryOperator(algorithm="mascara-landsat",
                                   version="1.0",
                                   lat=(10, 11),
                                   lon=(-75, -74),
                                   product="LS7_ETM_LEDAPS",
                                   time_ranges=("2014-01-01", "2014-12-31"),
                                   params={
                                       'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
                                   },
                                   queue='airflow_small', dag=dag, task_id="consulta_p_2014")

medianas_2013 = CDColFromFileOperator(algorithm="compuesto-temporal-medianas-wf",
                                      version="1.0",
                                      lat=(10, 11),
                                      lon=(-75, -74),
                                      product="LS7_ETM_LEDAPS",
                                      time_ranges=("2013-01-01", "2013-12-31"),
                                      params={
                                          'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
                                          'minValid': 1,
                                      },
                                      queue='airflow_small', dag=dag, task_id="medianas_2013")

medianas_2014 = CDColFromFileOperator(algorithm="compuesto-temporal-medianas-wf",
                                      version="1.0",
                                      lat=(10, 11),
                                      lon=(-75, -74),
                                      product="LS7_ETM_LEDAPS",
                                      time_ranges=("2014-01-01", "2014-12-31"),
                                      params={
                                          'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
                                          'minValid': 1,
                                      },
                                      queue='airflow_small', dag=dag, task_id="medianas_2014")

pca = CDColReduceOperator(algorithm="deteccion-cambios-pca-wf",
                          version="1.0", queue='airflow_small',
                          params={
                              'bands': ["blue", "green", "red", "nir", "swir1", "swir2"],
                          },
                          dag=dag, task_id="pca", )

consulta_2013 >> medianas_2013>>pca
consulta_2014 >> medianas_2014>>pca
