

_queues = [
    'airflow_small',
    'airflow_medium',
    'airflow_large',
    'airflow_xlarge'
]
def policy(task):
    if task.state == 'failed':
        if task.queue != 'airflow_xlarge':
            task.queue = _queues[_queues.index(task.queue)+1]