from connect import get_connection
from install_sid import *
from load_all import *
from log_config import get_logger
from pathlib import Path
from datetime import datetime
from suivi_technique import insert_suivi_run, insert_suivi_traitement
from launch_load_wrk import *
from profiling_runner import *
from launch_load_soc import *
from launch_create_views import run_create_views



def run_etl_pipeline(dates: list[str]):
    ################################################################

    # 0. Connexion à Snowflake

    # Chargement des variables d'environnement
    load_dotenv()
    # Connexion
    conn = get_connection()


    ################################################################

    # 1. Création des tables STG, WRK, SOC, TCH

    # Configuration du logger
    logger = get_logger("install_sid.log", console=True)

    # Ordre des scripts à exécuter
    SCRIPT_ORDER = [
        "01_create_databases.sql",
        "02_create_stg_tables.sql",
        "03_create_wrk_tables.sql",
        "04_create_tch_tables.sql",
        "05_create_soc_tables.sql"
    ]

    # Création des tables
    parser = argparse.ArgumentParser(description="Installe le SID médical dans Snowflake.")
    args = parser.parse_args()
    run_installation()


    ################################################################

    # 2. Enrichissement de la table STG

    for date_str in dates:
        load_files_by_date(date_str, conn)


    ################################################################

    # 3. Enrichissement de la table WRK
    # Configuration
    SQL_DIR = Path(__file__).resolve().parent.parent / "sql/_stg_to_wrk"
    logger = get_logger("load_wrk.log", console=True)

    # Exécution des scripts dans cet ordre
    exec_order = [
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

    """Exécute tous les scripts WRK avec génération de run_id / exec_id et suivi technique."""
    run_id = int(datetime.now().strftime("%Y%m%d%H%M%S"))
    run_start = datetime.now()
    logger.info(f"🆔 run_id = {run_id}")

    try:
        exec_id = 1
        for file_name in exec_order:
            file_path = SQL_DIR / file_name
            if file_path.exists():
                execute_sql_file_with_exec_id(conn, file_path, exec_id, run_id, logger)

                # PHASE EXPLORATOIRE INTERMEDIAIRE
                table_name = file_name.replace("_insert_", "").replace(".sql", "")
                logger.info(f"🔎 Exploration de la table {table_name}")

                query = f"SELECT * FROM BASE_WORK.PUBLIC.{table_name.upper()} LIMIT 10000"

                cursor = conn.cursor()
                try:
                    cursor.execute(query)
                    data = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df_wrk = pd.DataFrame(data, columns=columns)
                finally:
                    cursor.close()

                # Appel de cleaners
                explore_table(df_wrk, table_name)

                exec_id += 1
            else:
                logger.warning(f"⚠️ Fichier introuvable : {file_name}")

    except Exception as e:
        run_end = datetime.now()
        insert_suivi_run(conn, run_id, run_start, run_end, "KO")
        logger.error(f"⛔ Échec du chargement WRK : {e}")
        raise


    # Cleaning des données
    '''
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    
    logger = get_logger("profiling.log", log_level=logging.INFO, console=True)
    logger.info("Connection to Snowflake successful.")
    run_sql_script(conn, "../sql/profiling_wrk_complet.sql")
    '''

    ################################################################

    # 4. Enrichissement de la table SOC

    # Configuration
    SQL_DIR = Path(__file__).resolve().parent.parent / "sql/_wrk_to_soc"
    logger = get_logger("load_soc.log", console=True)

    # Ordre d'exécution (respecter les dépendances fonctionnelles)
    exec_order = [
        "_insert_party.sql",
        "_insert_room.sql",
        "_insert_medicine.sql",
        "_insert_individual.sql",
        "_insert_staff.sql",
        "_insert_telephone.sql",
        "_insert_address.sql",
        "_insert_treatment.sql",
        "_insert_consultation.sql",
        "_insert_hospitalization.sql"
    ]

    """Exécute les scripts WRK → SOC avec suivi technique."""
    run_id = int(datetime.now().strftime("%Y%m%d%H%M%S"))
    run_start = datetime.now()
    logger.info(f"🆔 run_id = {run_id}")

    conn = get_connection()
    try:
        exec_id = 1
        for file_name in exec_order:
            file_path = SQL_DIR / file_name
            if file_path.exists():
                execute_sql_file_with_exec_id(conn, file_path, exec_id, run_id, logger)
                exec_id += 1
            else:
                logger.warning(f"⚠️ Fichier introuvable : {file_name}")

        run_end = datetime.now()
        insert_suivi_run(conn, run_id, run_start, run_end, "OK")

    except Exception as e:
        run_end = datetime.now()
        insert_suivi_run(conn, run_id, run_start, run_end, "KO")
        logger.error(f"⛔ Échec du chargement SOC : {e}")
        raise

if __name__ == "__main__":
    dates = [
        "20240429", "20240430", "20240501",
        "20240502", "20240503", "20240504",
        "20240505", "20240506", "20240507", "20240508"
    ]
    run_etl_pipeline(dates)
    run_create_views()