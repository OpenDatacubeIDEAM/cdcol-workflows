from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow import utils as airflow_utils
class CompressFileSensor(BaseSensorOperator):
    """
    An example of a custom Sensor. Custom Sensors generally overload
    the `poke` method inherited from `BaseSensorOperator`
    """

    def __init__(self, *args, **kwargs):
        super(CompressFileSensor, self).__init__(*args, **kwargs)

    def poke(self, context):

        return True