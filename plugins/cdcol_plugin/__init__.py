# coding=utf8
from airflow.plugins_manager import AirflowPlugin
from cdcol_plugin.operators.query_operator import CDColQueryOperator
from cdcol_plugin.operators.from_file_operator import CDColFromFileOperator
from cdcol_plugin.operators.reduce_operator import CDColReduceOperator
from cdcol_plugin.operators.bash_executor_operator import CDColBashOperator
from cdcol_plugin.sensors.compress_file_sensor import CompressFileSensor
 
 
class CDColPlugin(AirflowPlugin):
    name = 'cdcol_plugin'
    operators=[CDColQueryOperator,CDColFromFileOperator,CDColReduceOperator,CDColBashOperator,CompressFileSensor]
