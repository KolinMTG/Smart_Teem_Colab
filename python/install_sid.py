import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import argparse
from log_config import get_logger


SQL_DIR = Path(__file__).resolve().parent.parent / "sql"
# Chargement des variables d'environnement
load_dotenv()

# Configuration du logger
logger = get_logger("install_sid.log", console=True)


# Ordre des scripts à exécuter
SCRIPT_ORDER = [
    "01_create_databases.sql",
    "02_create_stg_tables.sql",
    "03_create_wrk_tables.sql",
    "04_create_tch_tables.sql",
    "05_create_soc_tables.sql"
]

def connect_to_snowflake() -> snowflake.connector.SnowflakeConnection:
    """ Établit une connexion à Snowflake en utilisant les variables d'environnement.
    Returns:
        snowflake.connector.SnowflakeConnection: Connexion à Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE")
        )
        logger.info("Connexion à Snowflake réussie.")
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à Snowflake : {e}")
        raise

def execute_sql_file(conn : snowflake.connector.SnowflakeConnection, file_path : str) -> None:
    """ Exécute un fichier SQL dans Snowflake.
    Args:
        conn: Connexion à Snowflake.
        file_path: Chemin du fichier SQL à exécuter.
    """
    logger.info(f"Début d'exécution du script : {file_path.name}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        with conn.cursor() as cursor:
            for i, stmt in enumerate(statements, 1):
                try:
                    cursor.execute(stmt)
                    logger.info(f"Statement {i}/{len(statements)} exécuté avec succès.")
                except Exception as stmt_error:
                    logger.error(f"Erreur dans le statement {i}/{len(statements)} : {stmt_error}")
                    raise
        logger.info(f"Script terminé avec succès : {file_path.name}")
    except Exception as e:
        logger.error(f"Échec du script {file_path.name} avec erreur : {e}")
        raise

def run_installation() -> None:
    """ Fonction principale pour exécuter l'installation du SID."""
    logger.info("=== Début de l'installation du SID médical ===")
    conn = connect_to_snowflake()
    try:
        for script_name in SCRIPT_ORDER:
            script_path = SQL_DIR / script_name
            if not script_path.exists():
                logger.warning(f"Le fichier {script_path} n'existe pas, script ignoré.")
                continue
            execute_sql_file(conn, script_path)
        logger.info("✅ Tous les scripts ont été exécutés avec succès.")
    finally:
        conn.close()
        logger.info("Connexion à Snowflake fermée.")
        logger.info("=== Fin de l'installation ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Installe le SID médical dans Snowflake.")
    args = parser.parse_args()
    run_installation()
