from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent.parent / "sql/_wrk_to_soc"

SOC_SCRIPTS = [
    "_insert_party.sql",
    "_insert_room.sql",
    "_insert_medicine.sql",
    "_insert_individual.sql",
    "_insert_staff.sql",
    "_insert_telephone.sql",
    "_insert_address.sql",
    "_insert_treatment.sql",
    "_insert_consultation.sql",
    "_insert_hospitalization.sql",
]

def execute_sql_file(conn, content: str, logger, file_name: str):
    logger.info(f"▶️ Script {file_name}")
    cursor = conn.cursor()
    try:
        cursor.execute(content)
        logger.info(f"✅ Succès : {file_name}")
    finally:
        cursor.close()
