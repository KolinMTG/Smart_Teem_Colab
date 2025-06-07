
from pathlib import Path
from connect import get_connection
from insert_generic import insert_generic, insert_generic_upgrade
from log_config import get_logger

# Configuration du logger
logger = get_logger("loader_stg.log")


def load_files_by_date(date_str, conn):
    """Charge les fichiers de données pour une date spécifique dans la base de données.
    Args:
        date_str (str): La date au format 'YYYYMMDD'.
        conn: Connexion à la base de données.
    """
    base_dir = Path(__file__).resolve().parent.parent / "Data Hospital"
    folder = base_dir / f"BDD_HOSPITAL_{date_str}" #! Utiliser os.path.join pour la portabilité

    logger.info("────────────────────────────────────────────────────────")
    logger.info(f"🗓️  Début du chargement pour la date : {date_str}")

    if not folder.exists():
        logger.error(f"Dossier non trouvé : {folder.resolve()}")
        return

    for table in ["CHAMBRE", "CONSULTATION", "HOSPITALISATION", "MEDICAMENT", "PATIENT", "PERSONNEL", "TRAITEMENT"]:
        if table == "PATIENT":
            file_path = folder / f"{table}{date_str}.txt" #! Préférer un os.path.join ici pour la portabilité
        else:
            file_path = folder / f"{table}_{date_str}.txt" #! Même chose ici

        if file_path.exists():
            logger.info(f"➡️ Insertion de la table {table} depuis {file_path.name}")
            insert_generic_upgrade(file_path, conn, table)
        else:
            logger.warning(f"⚠️ Fichier {table} manquant : {file_path.name}")
