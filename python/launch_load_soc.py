"""Fichier d'exécution des scripts SQL pour le chargement des données de WRK vers SOC avec suivi technique."""

from log_config import get_logger
from connect import get_connection
from pathlib import Path
from datetime import datetime
from suivi_technique import insert_suivi_run, insert_suivi_traitement

# Configuration
SQL_DIR = Path(__file__).resolve().parent.parent / "sql/_wrk_to_soc"
logger = get_logger("load_soc.log", console=True)

# Ordre d'exécution (respecter les dépendances fonctionnelles)
exec_order = [
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

def execute_sql_file_with_exec_id(conn, file_path, exec_id: int, run_id: int, logger):
    """Exécute un script SQL avec injection de l'exec_id, et trace le traitement."""
    with open(file_path, encoding="utf-8") as f:
        content = f.read()

    logger.info(f"▶️ Exécution du script : {file_path.name} avec exec_id = {exec_id}")

    content = content.replace("%s", f"'{exec_id}'")

    script_start = datetime.now()
    cursor = conn.cursor()
    try:
        cursor.execute(content)
        logger.info(f"✅ Script {file_path.name} exécuté avec succès.")
        script_end = datetime.now()
        insert_suivi_traitement(conn, run_id, exec_id, file_path.name, script_start, script_end, "OK")
    except Exception as e:
        script_end = datetime.now()
        insert_suivi_traitement(conn, run_id, exec_id, file_path.name, script_start, script_end, "KO")
        logger.error(f"❌ Erreur dans {file_path.name} : {e}")
        raise
    finally:
        cursor.close()

def run():
    """Exécute les scripts WRK → SOC avec suivi technique."""
    run_id = int(datetime.now().strftime("%Y%m%d%H%M%S"))
    run_start = datetime.now()
    logger.info(f"🆔 run_id = {run_id}")

    conn = get_connection()
    try:
        exec_id = 1
        for file_name in exec_order:
            file_path = SQL_DIR / file_name
            if file_path.exists():
                execute_sql_file_with_exec_id(conn, file_path, exec_id, run_id, logger)
                exec_id += 1
            else:
                logger.warning(f"⚠️ Fichier introuvable : {file_name}")

        run_end = datetime.now()
        insert_suivi_run(conn, run_id, run_start, run_end, "OK")

    except Exception as e:
        run_end = datetime.now()
        insert_suivi_run(conn, run_id, run_start, run_end, "KO")
        logger.error(f"⛔ Échec du chargement SOC : {e}")
        raise
    finally:
        conn.close()
        logger.info("🔌 Connexion Snowflake fermée.")

if __name__ == "__main__":
    run()
