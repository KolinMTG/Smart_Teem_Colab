from connect import get_connection
from log_config import get_logger
from pathlib import Path
import os
from datetime import datetime

def run_create_views():
    ################################################################
    # Connexion à Snowflake
    conn = get_connection()

    ################################################################
    # Logger
    logger = get_logger("create_views.log", console=True)

    ################################################################
    # Dossier contenant les scripts SQL des vues
    SQL_DIR = Path(__file__).resolve().parent.parent / "sql/_views"
    logger.info(f"📁 Dossier SQL : {SQL_DIR}")

    # Vérification de l'existence du dossier
    if not SQL_DIR.exists():
        logger.error(f"⛔ Le dossier {SQL_DIR} n'existe pas.")
        return

    # Récupération des fichiers .sql
    sql_files = sorted([f for f in SQL_DIR.iterdir() if f.suffix == ".sql"])

    if not sql_files:
        logger.warning("⚠️ Aucun fichier SQL trouvé dans le dossier.")
        return

    ################################################################
    # Exécution des scripts SQL
    cursor = conn.cursor()
    try:
        for file_path in sql_files:
            view_name = file_path.stem
            with open(file_path, "r", encoding="utf-8") as f:
                sql_query = f.read()

            logger.info(f"Création de la vue : {view_name}")
            try:
                cursor.execute(sql_query)
                logger.info(f"✅ Vue {view_name} créée avec succès.")
            except Exception as e:
                logger.error(f"❌ Erreur lors de la création de {view_name} : {e}")

    finally:
        cursor.close()
        conn.close()
        logger.info("Connexion Snowflake fermée.")

