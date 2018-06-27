cubocubo

#En la máquina DB:
_DB_PASSWORD="cubocubo"
_IP="172.24.99.217"
sudo apt install redis-server
sudo sed -i "s%bind .*%bind $_IP%" "/etc/redis/redis.conf"
sudo service redis restart
sudo rabbitmqctl add_user airflow airflow
sudo rabbitmqctl add_vhost airflow
sudo rabbitmqctl set_permissions -p airflow airflow ".*" ".*" ".*"
sudo rabbitmq-plugins enable rabbitmq_management
sudo rabbitmqctl set_user_tags airflow airflow_tag administrator
sudo service rabbitmq-server restart
sudo -u postgres createdb airflow
sudo -u postgres createuser airflow
sudo -u postgres psql airflow -c "alter user airflow with encrypted password '$_DB_PASSWORD';"
sudo -u postgres psql airflow -c "grant all privileges on database airflow to airflow;"
sudo systemctl restart postgresql.service

#En la master:
_DB_PASSWORD="cubocubo"
_IP="172.24.99.217"
conda install -y psycopg2 redis-py
conda install -y -c conda-forge "airflow<1.9" 
if [[ -z "${AIRFLOW_HOME}" ]]; then
	export AIRFLOW_HOME="$HOME/airflow"
	echo "export AIRFLOW_HOME='$HOME/airflow'" >>"$HOME/.bashrc"
fi

airflow initdb
sed -i "s%sql_alchemy_conn.*%sql_alchemy_conn = postgresql+psycopg2://airflow:$_DB_PASSWORD@$_IP:5432/airflow%" "$AIRFLOW_HOME/airflow.cfg"
sed -i "s%executor =.*%executor = CeleryExecutor%" "$AIRFLOW_HOME/airflow.cfg"

sed -i "s%broker_url =.*%broker_url = amqp://airflow:airflow@$_IP/airflow%" "$AIRFLOW_HOME/airflow.cfg"
sed -i "s%celery_result_backend =.*%celery_result_backend = redis://$_IP:6379/0%" "$AIRFLOW_HOME/airflow.cfg"

sudo mkdir -p /web_storage/{dags,plugins}
ln -s /web_storage/dags "$AIRFLOW_HOME/dags"
ln -s /web_storage/plugins "$AIRFLOW_HOME/plugins"

cat <<EOF >"$AIRFLOW_HOME/dags/dummy.py"
import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import timedelta
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}
dag = DAG(
    dag_id='example_dummy', default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=1))
run_this_last = DummyOperator(task_id='DOES_NOTHING', dag=dag)
EOF

airflow initdb

#En los workers: 
_DB_PASSWORD="cubocubo"
_IP="172.24.99.217"
conda install -y psycopg2 redis-py
conda install -y -c conda-forge "airflow<1.9"
if [[ -z "${AIRFLOW_HOME}" ]]; then
	export AIRFLOW_HOME="$HOME/airflow"
	echo "export AIRFLOW_HOME='$HOME/airflow'" >>"$HOME/.bashrc"
fi
airflow initdb
sed -i "s%sql_alchemy_conn.*%sql_alchemy_conn = postgresql+psycopg2://airflow:$_DB_PASSWORD@$_IP:5432/airflow%" "$AIRFLOW_HOME/airflow.cfg"
sed -i "s%executor =.*%executor = CeleryExecutor%" "$AIRFLOW_HOME/airflow.cfg"

sed -i "s%broker_url =.*%broker_url = amqp://airflow:airflow@$_IP/airflow%" "$AIRFLOW_HOME/airflow.cfg"
sed -i "s%celery_result_backend =.*%celery_result_backend = redis://$_IP:6379/0%" "$AIRFLOW_HOME/airflow.cfg"


ln -s /web_storage/dags "$AIRFLOW_HOME/dags"
ln -s /web_storage/plugins "$AIRFLOW_HOME/plugins"