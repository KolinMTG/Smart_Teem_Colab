from dotenv import load_dotenv
import logging
import pandas as pd
from log_config import get_logger
from connect import get_connection
import re
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)

load_dotenv()

logger = get_logger("profiling.log", log_level=logging.INFO, console=True)


def run_sql_script(conn, script_path):
    with open(script_path, "r") as file:
        sql_script = file.read()
    queries = [q.strip() for q in sql_script.split(";") if q.strip()]

    cursor = conn.cursor()
    for i, query in enumerate(queries):
        try:
            # logger.info(f"Executing query {i+1}/{len(queries)}...")
            match = re.search(r'FROM\s+BASE_WORK\.PUBLIC\.([A-Z_]+)', query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                logger.info(f"==== 🗂️  Analyse de la table {table_name} ====")
            else:
                logger.info("==== 🗂️  Analyse d'une requête ====")

            cursor.execute(query)
            try:
                results = cursor.fetchall()
                if results:
                    df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
                    print(df)
            except Exception:
                logger.info("Query executed successfully (no result returned).")
        except Exception as e:
            logger.error(f"Error executing query {i+1}: {e}")
    cursor.close()

if __name__ == "__main__":
    try:
        conn = get_connection()
        logger.info("Connection to Snowflake successful.")
        run_sql_script(conn, "../sql/profiling_wrk_complet.sql")
        conn.close()
    except Exception as e:
        logger.error(f"Connection or execution failed: {e}")
