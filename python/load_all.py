import logging
from pathlib import Path
from connect import get_connection
from insert_generic import insert_generic

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def load_files_by_date(date_str, conn):
    base_dir = Path(__file__).resolve().parent.parent / "Data Hospital"
    folder = base_dir / f"BDD_HOSPITAL_{date_str}"

    logging.info("────────────────────────────────────────────────────────")
    logging.info(f"🗓️  Début du chargement pour la date : {date_str}")
    # logging.info(f"📁 Chemin du dossier attendu : {folder}")

    if not folder.exists():
        logging.error(f"Dossier non trouvé : {folder.resolve()}")
        return
    for table in ["CHAMBRE", "CONSULTATION", "HOSPITALISATION", "MEDICAMENT", "PATIENT", "PERSONNEL", "TRAITEMENT"]:
        # Gérer le nom du fichier selon la table
        if table == "PATIENT":
            file_path = folder / f"{table}{date_str}.txt"
        else:
            file_path = folder / f"{table}_{date_str}.txt"

        if file_path.exists():
            print(f"➡️ Insertion de la table {table} depuis {file_path}")
            insert_generic(file_path, conn, table)
        else:
            logging.warning(f"Fichier {table} manquant : {file_path.name}")