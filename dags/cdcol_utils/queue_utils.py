scores = {
    'compuesto-temporal-medianas-wf': 1,
    'mascara-landsat': 1,
    'ndsi-wf': 1,
    'ndvi-wf':1,
    'k-means-wf':1,
    'joiner':1,
    'test-reduce':1,
    'bosque-no-bosque-wf':1,
    'clasificador-generico-wf':1,
    'wofs-wf':1,
    'wofs-time-series-wf':1,
    'deteccion-cambios-pca-wf':1
}

def get_queue_by_year(time_range):
    start_date_value = datetime.datetime.strptime(start_date, "%d-%m-%Y")
    end_date_value = datetime.datetime.strptime(end_date, "%d-%m-%Y")
    anhos += 1 + (end_date_value.year - start_date_value.year)

    if anhos <=3:
        return 'airflow_small'
    elif anhos >= 4 and anhos <=8:
        return 'airflow_medium'
    elif anhos >= 9 and anhos <=15:
        return 'airflow_large'