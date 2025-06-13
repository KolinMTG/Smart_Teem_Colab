from log_config import get_logger
from pathlib import Path
from cleaners import explore_table
import pandas as pd

SQL_DIR = Path(__file__).resolve().parent.parent / "sql/_stg_to_wrk"

WRK_SCRIPTS = [
    "_insert_r_room.sql",
    "_insert_r_part.sql",
    "_insert_r_medc.sql",
    "_insert_o_tret.sql",
    "_insert_o_indv.sql",
    "_insert_o_stff.sql",
    "_insert_o_telp.sql",
    "_insert_o_addr.sql",
    "_insert_o_cons.sql",
    "_insert_o_hosp.sql"
]

def execute_sql_file(conn, content: str, logger, file_name: str):
    logger.info(f"▶️ Script {file_name}")
    cursor = conn.cursor()
    try:
        cursor.execute(content)
        logger.info(f"✅ Succès : {file_name}")
    finally:
        cursor.close()

def explore_table_after_script(conn, file_name: str, logger):
    table_name = file_name.replace("_insert_", "").replace(".sql", "")
    logger.info(f"🔎 Exploration table {table_name}")
    query = f"SELECT * FROM BASE_WORK.PUBLIC.{table_name.upper()} LIMIT 10000"

    cursor = conn.cursor()
    try:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        explore_table(df, table_name)
    finally:
        cursor.close()
