import logging

def insert_chambres(file_path, conn):
    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    headers = lines[0].strip().split(";")[1:]
    data = lines[1:]
    placeholders = ", ".join(["%s"] * len(headers))
    insert_query = f"INSERT INTO CHAMBRE ({', '.join(headers)}) VALUES ({placeholders})"

    with conn.cursor() as cursor:
        cursor.execute("USE WAREHOUSE COMPUTE_WH")

        for i, line in enumerate(data, 1):
            values = line.strip().split(";")[1:]
            if len(values) != len(headers):
                logging.warning(f"Ligne {i} ignorée : {values}")
                continue
            try:
                log_query = f"INSERT INTO CHAMBRE ({', '.join(headers)}) VALUES ({', '.join(values)})"
                logging.info(f"Requête : {log_query}")
                cursor.execute(insert_query, tuple(values))
                logging.info(f"Ligne {i} insérée avec succès.")
            except Exception as e:
                logging.error(f"Erreur à la ligne {i} : {e}")
