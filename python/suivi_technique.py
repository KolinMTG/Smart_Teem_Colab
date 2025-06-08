from pathlib import Path
from log_config import get_logger

logger = get_logger("load_wrk.log", console=True)

def insert_suivi_run(conn, run_id, start_time, end_time, status):
    sql_path = Path(__file__).resolve().parent.parent / "sql/_tch/_insert_t_suiv_run.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        query_template = f.read()

    query = query_template.format(
        run_id,
        start_time.strftime('%Y-%m-%d %H:%M:%S'),
        end_time.strftime('%Y-%m-%d %H:%M:%S'),
        status
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("USE DATABASE BASE_TECHNIQUE;")
            cursor.execute("USE SCHEMA PUBLIC;")
            cursor.execute(query)
        logger.info(f"📌 Insertion dans SUIVI_RUN réussie pour run_id = {run_id}")
    except Exception as e:
        logger.error(f"🚨 Erreur lors de l'insertion dans SUIVI_RUN : {e}")
        raise

def insert_suivi_traitement(conn, run_id, exec_id, script_name, start_time, end_time, status):
    sql_path = Path(__file__).resolve().parent.parent / "sql/_tch/_insert_t_suiv_trmt.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        query_template = f.read()

    query = query_template.format(
        run_id,
        exec_id,
        script_name,
        start_time.strftime('%Y-%m-%d %H:%M:%S'),
        end_time.strftime('%Y-%m-%d %H:%M:%S'),
        status
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("USE DATABASE BASE_TECHNIQUE;")
            cursor.execute("USE SCHEMA PUBLIC;")
            cursor.execute(query)
        logger.info(f"📌 Insertion dans SUIVI_TRAITEMENT réussie pour exec_id = {exec_id}")
    except Exception as e:
        logger.error(f"🚨 Erreur lors de l'insertion dans SUIVI_TRAITEMENT : {e}")
        raise
