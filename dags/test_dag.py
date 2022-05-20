from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
import logging

DEFAULT_ARGS = {
    "start_date": datetime(2021, 8, 17),
    "owner": "airflow",
    "depends_on_past": False
}

logging.basicConfig("%(asctime)s %(message)s")
log = logging.getLogger("name")

with DAG(
    dag_id="teste_date",
    schedule_interval=None,
    max_active_runs=1,
    default_args=DEFAULT_ARGS
) as dag:
    log.info("Teste: {{next_execution_date}}")
    operator = BashOperator(
        task_id = "teste_date", 
        bash_command="echo {{next_execution_date}}"
        )
    
    operator