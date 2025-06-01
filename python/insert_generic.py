import logging
from datetime import datetime

def convert_custom_timestamp(ts):
    try:
        # Format complet
        return datetime.strptime(ts, "%Y-%m-%d-%H-%M-%S").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            # Format date seul : on complète avec minuit
            return datetime.strptime(ts, "%Y-%m-%d").strftime("%Y-%m-%d 00:00:00")
        except Exception:
            # logging.warning(f"Timestamp invalide détecté (tentative de fallback) : {ts}")
            return None

def clean_timestamp_strict(value: str) -> str:
    if not value:
        return value
    value = value.strip()

    if "##" in value:
        # logging.warning(f"🛠️ Correction placeholder ## dans : {value}")
        value = value.replace("##", "00")

    if len(value) == 10:
        value += "-00-00-00"

    try:
        datetime.strptime(value, "%Y-%m-%d-%H-%M-%S")
    except ValueError:
        logging.warning(f"Format encore incorrect après nettoyage : {value}")
    return value

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

            timestamp_cols = [i for i, h in enumerate(headers) if h.startswith("TS_")]
            inserted = 0

            for i, line in enumerate(data, 1):
                values = line.strip().split(";")[1:]

                if not values or values[0].startswith("ID_"):
                    logging.warning(f"[{table_name}] 🟡 Ligne {i} ignorée (entête ou vide)")
                    continue

                if len(values) != expected_cols:
                    logging.warning(f"[{table_name}] ⚠️ Ligne {i} ignorée : {len(values)} colonnes vs {expected_cols} attendues")
                    continue

                # Correction des timestamps invalides
                for idx in timestamp_cols:
                    original_ts = values[idx]
                    original_ts = values[idx].strip()

                    # Cas 1 : NULL explicite ou vide → devient None
                    if original_ts.upper() == "NULL" or original_ts == "":
                        values[idx] = None
                        continue

                    # Cas 2 : tentative de conversion
                    parsed = convert_custom_timestamp(original_ts)
                    if parsed is None:
                        cleaned_ts = clean_timestamp_strict(original_ts)
                        parsed = convert_custom_timestamp(cleaned_ts)
                        values[idx] = parsed if parsed else cleaned_ts
                    else:
                        values[idx] = parsed

                try:
                    cursor.execute(insert_query, tuple(values))
                    inserted += 1
                except Exception as e:
                    logging.error(f"[{table_name}] ❌ Erreur à la ligne {i} : {e}")

            logging.info(f"[{table_name}] ✅ {inserted} lignes insérées avec succès.")
    except Exception as e:
        logging.error(f"[{table_name}] 🔥 Erreur générale : {e}")
        raise
