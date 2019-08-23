from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow import utils as airflow_utils
from airflow import models,settings
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException
import logging

logging.basicConfig(
    format='%(levelname)s : %(asctime)s : %(message)s',
    level=logging.DEBUG
)

class CompressFileSensor(BaseSensorOperator):
    """
    An example of a custom Sensor. Custom Sensors generally overload
    the `poke` method inherited from `BaseSensorOperator`
    """

    @airflow_utils.apply_defaults
    def __init__(self, execID,*args, **kwargs):
        self.mode = 'reschedule'
        self.poke_interval = 90
        self.soft_fail = True
        super(CompressFileSensor, self).__init__(*args, **kwargs)
        self.execID = execID

    def poke(self, context):
        print('SENSOR......COMPRESS')
        print(str(self.execID))
        dagbag = models.DagBag(settings.DAGS_FOLDER)
        dag = dagbag.get_dag(self.execID)
        dr_list = DagRun.find(dag_id=self.execID)
        dag_run = dr_list[-1]
        tasks_sucess = len(dag_run.get_task_instances(state=State.SUCCESS))
        tasks_failed = len(dag_run.get_task_instances(state=State.FAILED))
        tasks_skiped = len(dag_run.get_task_instances(state=State.SKIPPED))
        tasks_upstream_failed = len(dag_run.get_task_instances(state=State.UPSTREAM_FAILED))
        tasks_queued = len(dag_run.get_task_instances(state=State.QUEUED))
        tasks_removed = len(dag_run.get_task_instances(state=State.REMOVED))
        total_tasks = len(dag_run.get_task_instances())
        for task in dag_run.get_task_instances():
            print(task)
        print('Tasks success ' + str(tasks_sucess))
        print('Tasks failed ' +  str(tasks_failed))
        print('Tasks skiped ' + str(tasks_skiped))
        print('Tasks upstream_failed ' + str(tasks_upstream_failed))
        print('Tasks queued ' + str(tasks_queued))
        print('Total tasks ' + str(total_tasks))

        if (tasks_failed + tasks_sucess + tasks_skiped) == (total_tasks - tasks_removed - 2):
            print('Finished ' + str(True))
            return True
        elif ((tasks_failed + tasks_upstream_failed + tasks_skiped + tasks_sucess) == (total_tasks-tasks_removed - 2)):
            print("ERROR: Todas las tareas fallaron. No hay resultados para comprimir")
            raise AirflowSkipException("ERROR: Todas las tareas fallaron. No hay resultados para comprimir")
        else:
            print('Finished ' + str(False))
            return False
