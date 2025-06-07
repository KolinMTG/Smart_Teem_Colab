"""Contient des fonctions utiles pour l'exécution de scripts SQL dans Snowflake."""
import logging
import snowflake.connector
from pathlib import Path

def execute_sql_file(conn : snowflake.connector.SnowflakeConnection, file_path : str, logger: logging.Logger) -> None:
    """ Exécute un fichier SQL dans Snowflake.
    Args:
        conn: Connexion à Snowflake.
        file_path: Chemin du fichier SQL à exécuter.
        logger: Instance de logger pour enregistrer les messages.
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


def execute_sql_folder(
    conn: snowflake.connector.SnowflakeConnection,
    folder_path: str,
    logger: logging.Logger,
    exec_order: list[str] = None
) -> None:
    """Exécute des fichiers SQL dans un dossier dans un ordre défini par exec_order

    Args:
        conn: Connexion à Snowflake.
        folder_path: Dossier contenant les fichiers SQL.
        logger: Logger utilisé pour afficher les logs.
        exec_order: Liste de noms de fichiers SQL à exécuter dans l'ordre voulu.
    """
    path = Path(folder_path)
    if not path.is_dir():
        raise ValueError(f"{folder_path} n'est pas un dossier valide.")

    logger.info(f"📁 Exécution des scripts SQL dans le dossier : {folder_path}")

    if exec_order:
        # Exécution dans l’ordre défini par exec_order
        for file_name in exec_order:
            file_path = path / file_name
            if file_path.exists():
                execute_sql_file(conn, file_path, logger)
            else:
                logger.warning(f"⚠️ Le fichier spécifié dans exec_order est introuvable : {file_path.name}")
    else:
        # Fallback : exécution dans l’ordre alphabétique
        for file_path in sorted(path.glob("*.sql")):
            execute_sql_file(conn, file_path, logger)

    logger.info(f"✅ Tous les scripts dans {folder_path} ont été exécutés.")

