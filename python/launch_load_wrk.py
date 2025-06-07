# Script de chargement des données pour la table WRK
"""Fichier d'exécution des scripts SQL pour le chargement des données dans la table WRK."""

from log_config import get_logger
from connect import get_connection
from pathlib import Path
from datetime import datetime

# Configuration
SQL_DIR = Path(__file__).resolve().parent.parent / "sql/_stg_to_wrk"
logger = get_logger("load_wrk.log", console=True)

# Exécution des scripts dans cet ordre
exec_order = [
    "_insert_r_room.sql",
    "_insert_r_part.sql",
    "_insert_r_medc.sql",
    "_insert_o_tret.sql",
    "_insert_o_indv.sql",
    "_insert_o_stff.sql",
    "_insert_o_telp.sql",
    "_insert_o_addr.sql",
    "_insert_o_cons.sql",
    "_insert_o_hosp.sql"
    
]

def execute_sql_file_with_exec_id(conn, file_path,exec_id:int, logger):
    """Lit, remplace {{EXEC_ID}}, exécute le fichier SQL."""
    with open(file_path, encoding="utf-8") as f:
        content = f.read()


    logger.info(f"▶️ Exécution du script : {file_path.name} avec exec_id = {exec_id}")
    
    # Remplacement de l'placeholder dans le fichier
    content = content.replace("%s", f"'{exec_id}'")

    cursor = conn.cursor()
    try:
        cursor.execute(content)
        logger.info(f"✅ Script {file_path.name} exécuté avec succès.")
    except Exception as e:
        logger.error(f"❌ Erreur dans {file_path.name} : {e}")
        raise
    finally:
        cursor.close()

def run():
    """Exécute tous les scripts WRK avec génération de run_id / exec_id."""
    run_id = datetime.now().strftime("RUN_%Y%m%d_%H%M%S")
    logger.info(f"🆔 run_id = {run_id}")

    conn = get_connection()
    try:
        exec_id = 1
        for file_name in exec_order:
            file_path = SQL_DIR / file_name
            if file_path.exists():
                execute_sql_file_with_exec_id(conn, file_path, exec_id, logger)
                exec_id += 1
            else:
                logger.warning(f"⚠️ Fichier introuvable : {file_name}")
        logger.info("✅ Chargement complet WRK terminé.")
    except Exception as e:
        logger.error(f"⛔ Échec du chargement WRK : {e}")
        raise
    finally:
        conn.close()
        logger.info("🔌 Connexion Snowflake fermée.")

if __name__ == "__main__":
    run()
