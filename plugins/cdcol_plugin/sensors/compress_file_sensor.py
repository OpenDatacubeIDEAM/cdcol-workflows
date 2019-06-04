from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow import utils as airflow_utils
from airflow import models,settings
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.exceptions import AirflowException, AirflowSensorTimeout, AirflowSkipException
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
        print(self.execID)
        dagbag = models.DagBag(settings.DAGS_FOLDER)
        dag = dagbag.get_dag(self.execID)
        dr_list = DagRun.find(dag_id=self.execID)
        dag_run = dr_list[-1]
        tasks_sucess = len(dag_run.get_task_instances(state=State.SUCCESS))
        tasks_failed = len(dag_run.get_task_instances(state=State.FAILED))
        tasks_skiped = len(dag_run.get_task_instances(state=State.SKIPPED))
        tasks_upstream_failed = len(dag_run.get_task_instances(state=State.UPSTREAM_FAILED))
        tasks_queued = len(dag_run.get_task_instances(state=State.QUEUED))
        total_tasks = len(dag_run.get_task_instances())
        if (tasks_failed + tasks_sucess + tasks_skiped) == (total_tasks - 2):
            return True
        elif ((tasks_failed + tasks_upstream_failed + tasks_skiped + tasks_sucess) == (total_tasks - 2)):
            print("ERROR: Todas las tareas fallaron. No hay resultados para comprimir")
            raise AirflowSkipException("ERROR: Todas las tareas fallaron. No hay resultados para comprimir")
        else:
            return False