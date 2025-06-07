#script de chargement des données pour la table WRK
"""Fichier d'exécution des scriptes SQL pour le chargement des données dans la table WRK."""
from log_config import get_logger
from sql_executor import execute_sql_folder
from connect import get_connection
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent.parent / "sql/_stg_to_wrk"
# Configuration du logger
logger = get_logger("load_wrk.log", console=True)

def run():
    """Exécute les scripts SQL pour le chargement des données dans la table WRK."""
    conn = get_connection()
    exec_order = [
    "_insert_r_room.sql",
    "_insert_o_tret.sql",
    "_insert_r_part.sql",
    "_insert_o_indv.sql",
    "_insert_o_stff.sql",
    "_insert_o_telp.sql",
    "_insert_o_addr.sql",
    "_insert_o_cons.sql",
    "_insert_o_hosp.sql",
    "_insert_r_medc.sql"
]
    try:
        logger.info("📁 Exécution des scripts SQL pour le chargement des données dans la table WRK")
        execute_sql_folder(conn, SQL_DIR / "wrk", logger, exec_order=exec_order)
        logger.info("✅ Chargement des données dans la table WRK terminé avec succès.")
    except Exception as e:
        logger.error(f"❌ Échec du chargement des données dans la table WRK : {e}")
        raise
    finally:
        conn.close()
        logger.info("Connexion à Snowflake fermée.")


if __name__ == "__main__":
    run()



