from connect import get_connection
from install_sid import run_installation
from load_all import load_files_by_date
from log_config import get_logger
from pathlib import Path
from datetime import datetime
from suivi_technique import (
    insert_suivi_run,
    insert_suivi_traitement,
    update_suivi_traitement,
    get_next_exec_id
)
from reinit_stg_wrk import reinit_stg_and_wrk
from launch_load_wrk import WRK_SCRIPTS, SQL_DIR as WRK_DIR, execute_sql_file, explore_table_after_script
from launch_load_soc import execute_sql_file as execute_sql_soc
from dotenv import load_dotenv
from launch_create_views import run_create_views
from export_views_csv import export_views_to_csv
import sys

SOC_SCRIPTS = [
    "_insert_party.sql",
    "_insert_room.sql",
    "_insert_medicine.sql",
    "_insert_individual.sql",
    "_insert_staff.sql",
    "_insert_telephone.sql",
    "_insert_address.sql",
    "_insert_treatment.sql",
    "_insert_consultation.sql",
    "_insert_hospitalization.sql"
]

# Emplacements des dossiers SQL et des scripts de réinitialisation
SOC_DIR = Path(__file__).resolve().parent.parent / "sql/_wrk_to_soc"
REINIT_STG = Path(__file__).resolve().parent.parent / "sql/_reinitialisation/_reinit_stg.sql"
REINIT_WRK = Path(__file__).resolve().parent.parent / "sql//_reinitialisation/_reinit_wrk.sql"
REINIT_DB = Path(__file__).resolve().parent.parent / "sql//_reinitialisation/drop_databases.sql"

def run_etl_pipeline(dates: list[str], run_id_dag: str=None, already_installed: bool=False) -> None:
    load_dotenv()
    logger = get_logger("etl.log", console=True)
    ts_maj_personnel = datetime.now().replace(microsecond=0)

    # Étape 0 : réinitialisation complète des bases (DROP + CREATE)
    conn_init = get_connection()
    sql_db   = REINIT_DB.read_text(encoding="utf-8")
    logger.info("Réinitialisation complète des bases DROP")

    # On split sur ';' et on exécute chaque bloc non vide
    cursor = conn_init.cursor()
    for stmt in sql_db.split(';'):
        stmt = stmt.strip()
        if not stmt:
            continue
        logger.debug(f"▶️ Exécution SQL:\n{stmt[:50]}...")  # log de début de stmt
        cursor.execute(stmt)
    cursor.close()
    conn_init.close()
    
    # Nouvelle variable de contrôle pour exécuter run_installation() une seule fois
    #already_installed = False # Variable passée en paramètre avec la même valeur par défaut

    # Boucle principale : traiter chaque date indépendamment
    for date_str in dates:
        conn = get_connection()
        # Étape d'installation de la SID : exécutée uniquement lors de la première exécution du pipeline.
        # Elle crée toutes les bases de données (STG, WRK, SOC, TCH) ainsi que les tables associées.
        # Lors des exécutions suivantes, seules les tables des zones STG et WRK sont supprimées puis recréées.
        # Les tables des zones SOC et TCH, elles, ne sont pas recréées si elles existent déjà.
        if not already_installed:
            logger.info("=== Installation complète de la SID (bases + tables) ===")
            run_installation()
            already_installed = True
            logger.info("=== SID installée avec succès ===")
        else:
            logger.info("=== SID déjà installée : on réinitialise STG et WRK ===")
            reinit_stg_and_wrk()


        # Générer un identifiant unique pour ce run à partir de la date/heure
        if run_id_dag != None: # Si run_id est passé en paramètre, cela signifie que la fonction
            # est appelée par le DAG. Dans ce cas, une seule date est passée en paramètre.
            run_id = run_id_dag
        else:
            run_id = str(int(datetime.now().strftime("%Y%m%d%H%M%S"))) # type de run_id modifié au format string pour récupérer le run_id du DAG
        run_start = datetime.now()
        status = "OK"
        logger.info(f"--- Date {date_str} | run_id {run_id} ---")

        try:
            # Étape 3 : charger les fichiers bruts dans STAGING
            logger.info("=== Début du chargement des fichiers bruts dans STAGING ===")
            load_files_by_date(date_str, conn)
            logger.info("✅ Fichiers chargés dans STAGING pour la date : " + date_str)


            logger.info("=== Début du passage STG ➜ WRK ===")


            # Étape 5 : exécuter les scripts WRK avec suivi et exploration des valeurs
            exec_id = get_next_exec_id(conn)
            for file_name in WRK_SCRIPTS:
                file_path = WRK_DIR / file_name
                if not file_path.exists():
                    logger.warning(f"Fichier manquant WRK : {file_name}")
                    continue

                content = file_path.read_text(encoding="utf-8").replace("%s", f"'{exec_id}'")
                content = content.replace("%time_now",  f"'{ts_maj_personnel}'")
                script_start = datetime.now()
                insert_suivi_traitement(conn, run_id, exec_id, file_name, script_start, "ENC")

                try:
                    execute_sql_file(conn, content, logger, file_name)
                    status_script = "OK"
                except Exception as e:
                    status_script = "KO"
                    logger.error(f"❌ Erreur {file_name} : {e}")
                    raise
                finally:
                    script_end = datetime.now()
                    update_suivi_traitement(conn, run_id, exec_id, script_end, status_script)

                explore_table_after_script(conn, file_name, logger)
                exec_id += 1
            
            logger.info("=== Fin du passage STG ➜ WRK ===")

            # Étape 6 : exécuter les scripts SOC avec suivi
            logger.info("=== Début du passage WRK ➜ SOC ===")
            for file_name in SOC_SCRIPTS:
                file_path = SOC_DIR / file_name
                if not file_path.exists():
                    logger.warning(f"Fichier manquant SOC : {file_name}")
                    continue

                content = file_path.read_text(encoding="utf-8").replace("%s", f"'{exec_id}'")
                script_start = datetime.now()
                insert_suivi_traitement(conn, run_id, exec_id, file_name, script_start, "ENC")

                try:
                    execute_sql_soc(conn, content, logger, file_name)
                    status_script = "OK"
                except Exception as e:
                    status_script = "KO"
                    logger.error(f"❌ Erreur {file_name} : {e}")
                    raise
                finally:
                    script_end = datetime.now()
                    update_suivi_traitement(conn, run_id, exec_id, script_end, status_script)

                exec_id += 1
            logger.info("✅ Fin du passage WRK ➜ SOC")

        except Exception as e:
            status = "KO"
            logger.error(f"❌ Erreur sur {date_str} : {e}")
            raise
        finally:
            run_end = datetime.now()
            insert_suivi_run(conn, run_id, run_start, run_end, status)
            
            conn.close()


if __name__ == "__main__":

    # Le fichier est exécuté manuellement
    if len(sys.argv) == 1:
        dates = [
            "20240429", "20240430", "20240501", "20240502",
            "20240503", "20240504", "20240505", "20240506",
            "20240507", "20240508"
        ]
        dates = ["20240429"]
        run_etl_pipeline(dates)
        #créer les vues et exporter en csv
        run_create_views()
        views = [
            "VW_AVG_AGE_BY_PATHOLOGY",
            "VW_TOP_MEDICATION_BY_PATHOLOGY",
            "VW_ROOMS_BY_PATHOLOGY",
            "VW_DOCTOR_SPECIALTY_BY_PATHOLOGY",
            "VW_PATIENTS_ONE_NIGHT",
            "VW_EMPTY_ROOMS"
        ]
        export_views_to_csv(
            views=views,
            database="BASE_SOCLE",
            schema="PUBLIC",
            output_dir="csv"
        )

    # Le fichier est exécuté par le DAG de chargement
    elif len(sys.argv) == 4:

        # Gestion des arguments passés en ligne de commande
        print("Liste des arguments :", sys.argv)

        # Est-ce que le run_id reçu est le run_id du DAG ou son timestamp ?
        already_installed = False
        if eval(sys.argv[3]): # True si on a le run_id
            run_id: str = sys.argv[2]
            print(f"Paramètres de run_etl_pipeline (avec le 'run_id' du DAG) : {[sys.argv[1]]}, {run_id}, {already_installed}")
        else:
            run_id: str = str(int(datetime.fromisoformat(sys.argv[2]).strftime('%Y%m%d%H%M%S')))
            print(f"Paramètres de run_etl_pipeline (avec le 'ts' du DAG) : {[sys.argv[1]]}, {run_id}, {already_installed}")

        # Chargement d'un jour dans la base de données
        run_etl_pipeline([sys.argv[1]], run_id, already_installed)

    # Mauvais nombre d'arguments
    else:
        print("Erreur : mauvais nombre d'arguments.")
        sys.exit(0)

def obtenir_date(date_str_param: str):
    try:
        # On tente de parser la chaîne au format AAAAMMJJ
        datetime.strptime(date_str_param, "%Y%m%d")
    except ValueError:    
        # Arrêt du programme
        raise AirflowException("Date renseignée invalide (format : AAAAMMJJ).") # Ajout d'un message à l'exception
    return date_str_param

def obtenir_run_id(timestamp: str, dag_run_id: str):
    if USE_DAG_RUN_ID:
        run_id = dag_run_id
    else:
        # Reformatter le timestamp en "%Y%m%d%H%M%S"
        run_id = datetime.fromisoformat(timestamp).strftime("%Y%m%d%H%M%S")
    return run_id