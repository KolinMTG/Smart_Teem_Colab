# reinit_stg_wrk.py

from connect import get_connection
from log_config import get_logger
from pathlib import Path

REINIT_DIR = Path(__file__).resolve().parent.parent / "sql/_reinitialisation"
REINIT_STG = REINIT_DIR / "_reinit_stg.sql"
REINIT_WRK = REINIT_DIR / "_reinit_wrk.sql"

logger = get_logger("reinit_stg_wrk.log", console=True)

def execute_reinit_script(conn, file_path):
    logger.info(f"🔁 Réinitialisation via {file_path.name}")
    sql_text = file_path.read_text(encoding="utf-8")
    cursor = conn.cursor()
    for stmt in sql_text.split(";"):
        stmt = stmt.strip()
        if stmt:
            try:
                logger.debug(f"▶️ {stmt[:80]}...")
                cursor.execute(stmt)
            except Exception as e:
                logger.error(f"❌ Erreur dans {file_path.name} : {e}")
                raise
    cursor.close()

def reinit_stg_and_wrk():
    logger.info("=== Réinitialisation des tables STG et WRK ===")
    conn = get_connection()
    try:
        execute_reinit_script(conn, REINIT_STG)
        execute_reinit_script(conn, REINIT_WRK)
        logger.info("✅ Tables STG et WRK réinitialisées avec succès.")
    finally:
        conn.close()
        logger.info("🔒 Connexion fermée après réinitialisation.")
