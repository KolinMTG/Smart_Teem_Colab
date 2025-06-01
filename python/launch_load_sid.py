import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
import logging

# Chargement des variables d'environnement
load_dotenv()

# Configuration des chemins
DATA_FILE = Path(__file__).resolve().parent.parent / "Data Hospital/BDD_HOSPITAL_20240429" / "CHAMBRE_20240429.txt"
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "load_chambres.log"

# Configuration du logger
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', '%H:%M:%S'))
logging.getLogger().addHandler(console)

def connect_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            database=os.getenv("SNOWFLAKE_DATABASE")
        )
        logging.info("Connexion à Snowflake réussie.")
        return conn
    except Exception as e:
        logging.error(f"Erreur de connexion à Snowflake : {e}")
        raise

def insert_chambres(file_path, conn):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        headers = lines[0].strip().lstrip(";").split(";")
        data = lines[1:]

        insert_query = "INSERT INTO CHAMBRES ({cols}) VALUES ({placeholders})".format(
            cols=", ".join(headers),
            placeholders=", ".join(["%s"] * len(headers))
        )

        with conn.cursor() as cursor:
            for i, line in enumerate(data, 1):
                values = line.strip().split(";")
                try:
                    cursor.execute(insert_query, tuple(values))
                    logging.info(f"Ligne {i} insérée avec succès.")
                except Exception as e:
                    logging.error(f"Erreur à la ligne {i} : {e}")
        logging.info("✅ Insertion terminée.")
    except Exception as e:
        logging.error(f"Erreur lors du traitement du fichier : {e}")
        raise

def main():
    logging.info("=== Début du chargement des chambres ===")
    conn = connect_to_snowflake()
    try:
        insert_chambres(DATA_FILE, conn)
    finally:
        conn.close()
        logging.info("Connexion Snowflake fermée.")
        logging.info("=== Fin du chargement ===")

if __name__ == "__main__":
    main()
