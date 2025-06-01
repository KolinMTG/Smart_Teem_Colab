import logging
from datetime import datetime

def convert_custom_timestamp(ts):
    try:
        return datetime.strptime(ts, "%Y-%m-%d-%H-%M-%S").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        logging.warning(f"Timestamp invalide ou mal formaté : {ts}")
        return ts  # On garde tel quel si conversion impossible

def insert_generic(file_path, conn, table_name):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        headers = lines[0].strip().split(";")[1:]
        data = lines[1:]
        expected_cols = len(headers)
        placeholders = ", ".join(["%s"] * expected_cols)
        insert_query = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"

        with conn.cursor() as cursor:
            cursor.execute("USE WAREHOUSE COMPUTE_WH")

            # Détection des colonnes timestamp
            timestamp_cols = [i for i, h in enumerate(headers) if h.startswith("TS_")]
            inserted = 0

            for i, line in enumerate(data, 1):
                values = line.strip().split(";")[1:]

                if not values or values[0].startswith("ID_"):
                    logging.warning(f"[{table_name}] Ligne {i} ignorée : header dupliqué ou vide")
                    continue

                if len(values) != expected_cols:
                    logging.warning(f"[{table_name}] Ligne {i} ignorée : {len(values)} colonnes vs {expected_cols} attendues")
                    continue

                # Conversion des timestamps
                for idx in timestamp_cols:
                    values[idx] = convert_custom_timestamp(values[idx])

                try:
                    cursor.execute(insert_query, tuple(values))
                    inserted += 1
                except Exception as e:
                    logging.error(f"[{table_name}] Erreur à la ligne {i} : {e}")

            logging.info(f"[{table_name}] ✅ {inserted} lignes insérées avec succès.")
    except Exception as e:
        logging.error(f"[{table_name}] Erreur générale : {e}")
        raise
