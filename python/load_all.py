import logging
from pathlib import Path
from connect import get_connection
from insert_generic import insert_generic

# Définir le dossier "log" et le fichier de log
log_dir = Path(__file__).resolve().parent / "logs"
log_file = log_dir / "loader_stg.log"

# S'assurer que le dossier log existe (au cas où)
log_dir.mkdir(parents=True, exist_ok=True)

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(),  # Console
        logging.FileHandler(log_file, mode='a', encoding='utf-8')  # Fichier dans le dossier "log"
    ]
)

def load_files_by_date(date_str, conn):
    base_dir = Path(__file__).resolve().parent.parent / "Data Hospital"
    folder = base_dir / f"BDD_HOSPITAL_{date_str}"

    logging.info("────────────────────────────────────────────────────────")
    logging.info(f"🗓️  Début du chargement pour la date : {date_str}")

    if not folder.exists():
        logging.error(f"Dossier non trouvé : {folder.resolve()}")
        return

    for table in ["CHAMBRE", "CONSULTATION", "HOSPITALISATION", "MEDICAMENT", "PATIENT", "PERSONNEL", "TRAITEMENT"]:
        if table == "PATIENT":
            file_path = folder / f"{table}{date_str}.txt"
        else:
            file_path = folder / f"{table}_{date_str}.txt"

        if file_path.exists():
            logging.info(f"➡️ Insertion de la table {table} depuis {file_path.name}")
            insert_generic(file_path, conn, table)
        else:
            logging.warning(f"⚠️ Fichier {table} manquant : {file_path.name}")
