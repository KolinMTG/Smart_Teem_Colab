from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import logging, os


dossier_dag = os.path.dirname(os.path.abspath(__file__)) + "/install_sid.py"

def fonction_tache_1():
    logger = logging.getLogger(__name__)
    logger.info(f"Exécution du fichier python/install_sid.py")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_installation',
    default_args=default_args,
    description='Création de la base de données',
    schedule_interval=None,
)

# Tâches

tache_debut = DummyOperator(
    task_id='debut_pipeline',
    dag=dag,
)

tache_fin = DummyOperator(
    task_id='fin_pipeline',
    dag=dag,
)

tache_1 = PythonOperator(
    task_id='log_run_installation',
    python_callable=fonction_tache_1,
    dag=dag,
)

tache_2 = BashOperator(
    task_id="call_run_installation",
    bash_command=f"python {dossier_dag}",
)

# Définition de l'ordre d'exécution
tache_debut >> tache_1 >> tache_2 >> tache_fin