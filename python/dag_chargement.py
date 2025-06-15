from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
import logging, os
#from main import run_etl_pipeline

USE_DAG_RUN_ID = False

dossier_dag = os.path.dirname(os.path.abspath(__file__)) + "/main.py"

def fonction_tache_1(date_str_param, timestamp, dag_run_id):
    logger = logging.getLogger(__name__)
    
    # Récupérer la chaîne de caractères saisie par l'utilisateur
    logger.info(f"Paramètres reçus : date_str_param = {date_str_param}, timestamp = {timestamp}, dag_run_id = {dag_run_id}")

    # Appeler la fonction avec les deux paramètres
    logger.info("Exécution du fichier python/main.py")

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
    'dag_chargement',
    default_args=default_args,
    description='Chargement de la base de données pour un jour',
    schedule_interval=timedelta(days=1),
    params={  # Définition des paramètres dynamiques
        "date_str": "20240429"
    }
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
    task_id='run_etl_pipeline',
    python_callable=fonction_tache_1,
    # Utilisation de op_kwargs et de la templating Jinja pour passer le paramètre
    op_kwargs={
        'date_str_param': '{{ dag_run.conf.get("date_str", "valeur_par_defaut") }}',
        'timestamp': '{{ ts }}',
        'dag_run_id': '{{ run_id }}',
    },
    dag=dag,
)

tache_2 = BashOperator(
    task_id="bash_example",
    bash_command=\
            f'python {dossier_dag} ' + '{{ dag_run.conf.get("date_str", "valeur_par_defaut") }} {{ run_id }} True'\
        if USE_DAG_RUN_ID\
        else\
            f'python {dossier_dag} ' + '{{ dag_run.conf.get("date_str", "valeur_par_defaut") }} {{ ts }} False'
)

# --- Définition de l'ordre d'exécution ---
tache_debut >> tache_1 >> tache_2 >> tache_fin