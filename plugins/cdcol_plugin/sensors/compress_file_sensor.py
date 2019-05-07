from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow import utils as airflow_utils
from airflow import models,settings
from airflow.models import DagRun
from airflow.utils.state import State
class CompressFileSensor(BaseSensorOperator):
    """
    An example of a custom Sensor. Custom Sensors generally overload
    the `poke` method inherited from `BaseSensorOperator`
    """

    def __init__(self, execID, *args, **kwargs):
        super(CompressFileSensor, self).__init__(*args, **kwargs)
        self.execID = execID

    def poke(self, context):
        print(self.execID)
        dagbag = models.DagBag(settings.DAGS_FOLDER)
        dag = dagbag.get_dag(self.execID)
        dr_list = DagRun.find(dag_id=execution.dag_id)
        dag_run=dr_list[-1]
        tasks_sucess = len(dag_run.get_task_instances(state=State.SUCCESS))
        tasks_failed = len(dag_run.get_task_instances(state=State.FAILED))
        tasks_skiped = len(dag_run.get_task_instances(state=State.SKIPED))
        total_tasks = len(dag_run.get_task_instances())
        return (tasks_failed+tasks_sucess+tasks_skiped)==(total_tasks-2)