scores = {
    'small':(0,2) ,
    'medium':(3,8)
}

def get_queue_by_year(time_range, algoritmo, ):
    start_date_value = datetime.datetime.strptime(start_date, "%d-%m-%Y")
    end_date_value = datetime.datetime.strptime(end_date, "%d-%m-%Y")
    anhos += 1 + (end_date_value.year - start_date_value.year)

    if anhos <=2:
        return 'airflow_small'
    elif anhos >= 3 and anhos <=8:
        return 'airflow_medium'
    elif anhos >= 9 and anhos <=15:
        return 'airflow_large'