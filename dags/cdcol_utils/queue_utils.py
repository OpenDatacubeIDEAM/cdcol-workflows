import datetime

scores = {
    'small':range(0,2) ,
    'medium':range(3,4),
    'large':range(5,7),
    'xlarge':range(8,18)
}

def get_queue_by_year(time_range, entrada_multi_temporal,tiles ):
    start_date_value = datetime.datetime.strptime(time_range[0], "%Y-%m-%d")
    end_date_value = datetime.datetime.strptime(time_range[1], "%Y-%m-%d")
    anhos = 1 + (end_date_value.year - start_date_value.year)
    score = (anhos*tiles) if entrada_multi_temporal else anhos;
    if score in scores['small']:
        return 'airflow_small'
    elif scores['medium']:
        return 'airflow_medium'
    elif scores['large']:
        return 'airflow_large'
    else:
        return 'airflow_xlarge'