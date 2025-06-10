"""insert_generic.py appelé dans load_all.py
pour insérer des données dans une table générique de Snowflake."""

import os
import csv
import tempfile
import logging
from datetime import datetime


def convert_custom_timestamp(ts):
    """Convertit un timestamp personnalisé en format standard."""
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
    """Insère des données dans une table générique de Snowflake à partir d'un fichier texte.
    Args:
        file_path (str): Chemin du fichier texte contenant les données.
        conn: Connexion à la base de données Snowflake.
        table_name (str): Nom de la table cible dans Snowflake.
    """
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
                print(f"Traitement de la ligne {i} dans {table_name}...")
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


def insert_generic_upgrade(file_path, conn, table_name, stage_name="@my_internal_stage"):
    """
    #! Censé être une version améliorée de insert_generic
    Insère des données dans une table Snowflake à partir d'un fichier texte via stage.
    Reproduit exactement la logique de 'insert_generic', mais via un stage + COPY INTO.

    Args:
        file_path (str): Chemin du fichier texte original.
        conn: Connexion Snowflake.
        table_name (str): Nom de la table cible.
        stage_name (str): Stage interne (ex: '@my_internal_stage').
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        headers = lines[0].strip().split(";")[1:]  # skip 1ère colonne type ID
        data = lines[1:]
        # Supprimer ID_TRAITEMENT si table "consultation"
        idx_to_remove = None
        if table_name.lower() == "consultation":
            if "ID_TRAITEMENT" in headers:
                idx_to_remove = headers.index("ID_TRAITEMENT")
                headers.pop(idx_to_remove)

        expected_cols = len(headers)
        timestamp_cols = [i for i, h in enumerate(headers) if h.startswith("TS_")]

        # Crée un fichier CSV temporaire pour le bulk load
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv", newline='', encoding="utf-8") as tmp_csv:
            writer = csv.writer(tmp_csv, delimiter=';', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)
            inserted = 0

            for i, line in enumerate(data, 1):
                print(f"Traitement de la ligne {i} dans {table_name}...")
                values = line.strip().split(";")[1:]  # skip colonne 0
                # Supprimer la colonne ID_TRAITEMENT si nécessaire
                if idx_to_remove is not None:
                    values.pop(idx_to_remove)

                if not values or values[0].startswith("ID_"):
                    logging.warning(f"[{table_name}] 🟡 Ligne {i} ignorée (entête ou vide)")
                    continue

                if len(values) != expected_cols:
                    logging.warning(f"[{table_name}] ⚠️ Ligne {i} ignorée : {len(values)} colonnes vs {expected_cols} attendues")
                    continue

                # Traitement des timestamps
                for idx in timestamp_cols:
                    original_ts = values[idx].strip()
                    if original_ts.upper() == "NULL" or original_ts == "":
                        values[idx] = ""
                        continue

                    parsed = convert_custom_timestamp(original_ts)
                    if parsed is None:
                        cleaned_ts = clean_timestamp_strict(original_ts)
                        parsed = convert_custom_timestamp(cleaned_ts)
                        values[idx] = parsed if parsed else cleaned_ts
                    else:
                        values[idx] = parsed

                writer.writerow(values)
                inserted += 1

        # Upload du fichier dans le stage
        tmp_filename = os.path.basename(tmp_csv.name)
        with conn.cursor() as cursor:
            cursor.execute("USE WAREHOUSE COMPUTE_WH")
            cursor.execute(f"CREATE STAGE IF NOT EXISTS {stage_name.strip('@')}")
            logging.info(f"🔼 PUT {tmp_filename} vers {stage_name}")
            cursor.execute(f"PUT file://{tmp_csv.name} {stage_name} OVERWRITE = TRUE")

            # COPY INTO vers la table cible
            logging.info(f"📥 COPY INTO {table_name} depuis {stage_name}/{tmp_filename}")
            copy_cmd = f"""
            COPY INTO {table_name}
            FROM {stage_name}/{tmp_filename}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 FIELD_DELIMITER = ';')
            ON_ERROR = 'CONTINUE'
            """
            cursor.execute(copy_cmd)

        logging.info(f"[{table_name}] ✅ {inserted} lignes insérées via COPY INTO.")

        # Nettoyage
        os.remove(tmp_csv.name)

    except Exception as e:
        logging.error(f"[{table_name}] 🔥 Erreur générale : {e}")
        raise

