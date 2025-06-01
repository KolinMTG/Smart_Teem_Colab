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
    # for table in ["CHAMBRE", "CONSULTATION", "HOSPITALISATION", "MEDICAMENT", "PATIENT", "PERSONNEL", "TRAITEMENT"]:
    for table in ["CONSULTATION", "HOSPITALISATION", "MEDICAMENT", "PATIENT", "PERSONNEL", "TRAITEMENT"]:
        file_path = folder / f"{table}_{date_str}.txt"

        # logging.info("--------------------------------------------------------")
        # logging.info(f"🔍 Vérification du fichier : {file_path.name}")
        # logging.info(f"📂 Chemin complet : {file_path.resolve()}")

        if file_path.exists():
            # logging.info(f"Fichier trouvé : {file_path.name}")
            print(f"➡️ Insertion de la table {table} depuis {file_path}")
            insert_generic(file_path, conn, table)
        else:
            logging.warning(f"Fichier {table} manquant : {file_path.name}")


def main():
    # 🗓 Liste des dates à traiter
    dates = [
        "20240429",
        #"20240430",
        #"20240501",
        #"20240502",
        #"20240503",
        #"20240504",
        #"20240505",
        #"20240506",
        #"20240507",
        #"20240508",
    ]
    conn = get_connection()
    try:
        for date_str in dates:
            load_files_by_date(date_str, conn)
    finally:
        conn.close()
        logging.info("Connexion Snowflake fermée.")

if __name__ == "__main__":
    main()
