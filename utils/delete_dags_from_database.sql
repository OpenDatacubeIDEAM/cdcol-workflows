
create temporary table t_dags_to_remove as select dag_id from dag where dag_id like '%something%';
delete from xcom where dag_id in (select * from t_dags_to_remove);
delete from task_instance where dag_id in (select * from t_dags_to_remove);
delete from sla_miss where dag_id in (select * from t_dags_to_remove);
delete from log where dag_id in (select * from t_dags_to_remove);
delete from job where dag_id in (select * from t_dags_to_remove);
delete from dag_run where dag_id in (select * from t_dags_to_remove);
delete from dag where dag_id in (select * from t_dags_to_remove);
