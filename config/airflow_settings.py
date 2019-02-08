from airflow import task

def policy(task):
    if task.state == 'failed':
        print("Esto es un INTENTO")
        print(task.__class__.__name__)