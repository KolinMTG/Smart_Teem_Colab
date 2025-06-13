from datetime import datetime
from log_config import get_logger

logger = get_logger("suivi_technique.log", console=True)

def insert_suivi_run(conn, run_id: int,
                     start_time: datetime,
                     end_time: datetime,
                     statut: str) -> None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO BASE_TECHNIQUE.PUBLIC.SUIVI_RUN
            (RUN_ID, RUN_STRT_DTTM, RUN_END_DTTM, RUN_STTS_CD)
            VALUES (%s, %s, %s, %s)
            """,
            (
                run_id,
                start_time.strftime('%Y-%m-%d %H:%M:%S'),
                end_time.strftime('%Y-%m-%d %H:%M:%S'),
                statut
            )
        )
        conn.commit()
        logger.info(f"📌 SUIVI_RUN inséré | run_id={run_id} | statut={statut}")
    finally:
        cur.close()


def insert_suivi_traitement(conn, run_id, exec_id, script_name, start_time,statut):
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO BASE_TECHNIQUE.PUBLIC.SUIVI_TRAITEMENT 
            (RUN_ID, EXEC_ID, SCRPT_NAME, EXEC_STRT_DTTM,EXEC_STTS_CD)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                run_id,
                exec_id,
                script_name,
                start_time.strftime('%Y-%m-%d %H:%M:%S'),
                #end_time.strftime('%Y-%m-%d %H:%M:%S'),
                statut
            )
        )
        conn.commit()
        logger.info(f"📝 [INSERT] exec_id={exec_id} | {script_name} | statut={statut}")
    finally:
        cursor.close()



def update_suivi_traitement(conn, run_id, exec_id, end_time, statut):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            UPDATE BASE_TECHNIQUE.PUBLIC.SUIVI_TRAITEMENT
            SET EXEC_END_DTTM = %s,
                EXEC_STTS_CD = %s
            WHERE RUN_ID = %s AND EXEC_ID = %s
            """,
            (
                end_time.strftime('%Y-%m-%d %H:%M:%S'),
                statut,
                run_id,
                exec_id
            )
        )
        conn.commit()
        logger.info(f"🔄 [UPDATE] exec_id={exec_id} → statut={statut}")
    finally:
        cursor.close()


def get_next_exec_id(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COALESCE(MAX(EXEC_ID), 0) + 1 FROM BASE_TECHNIQUE.PUBLIC.SUIVI_TRAITEMENT")
        (next_id,) = cursor.fetchone()
        logger.info(f"🔢 Prochain EXEC_ID global = {next_id}")
        return int(next_id)
    finally:
        cursor.close()
