from load_all import *

def main():
    # 🗓 Liste des dates à traiter
    dates = [
        "20240429",
        "20240430",
        "20240501",
        "20240502",
        "20240503",
        "20240504",
        "20240505",
        "20240506",
        "20240507",
        "20240508",
    ]
    conn = get_connection()
    try:
        for date_str in dates:
            load_files_by_date(date_str, conn)
    finally:
        conn.close()
        logging.info("Connexion Snowflake fermée.")

if __name__ == "__main__":
    main()
