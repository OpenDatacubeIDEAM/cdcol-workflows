_queues = [
    'airflow_small',
    'airflow_medium',
    'airflow_large',
    'airflow_xlarge'
]
def policy(task_instance):
    if task_instance.task.state == 'failed':
        task_instance.task.queue = _queues[_queues.index(task_instance.task.queue)+1]