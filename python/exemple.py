# Ici un exemple de code Python et sql pour la mise en place des EXEC ID.

def execute_sql_file(
    conn: snowflake.connector.SnowflakeConnection, 
    file_path: str, 
    logger: logging.Logger, 
    params: tuple = None
) -> None:
    """
    Exécute un fichier SQL dans Snowflake.
    
    Args:
        conn: Connexion à Snowflake.
        file_path: Chemin du fichier SQL à exécuter.
        logger: Instance de logger pour enregistrer les messages.
        params: Tuple de paramètres à passer à la requête SQL (optionnel).
    """
    logger.info(f"Début d'exécution du script : {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # On sépare les statements par ';' pour exécuter un par un
        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        with conn.cursor() as cursor:
            for i, stmt in enumerate(statements, 1):
                try:

                    #! -------------------- ICI --------------------- #

                    if params and i == len(statements):  # passe les params uniquement au dernier statement (ou adapte selon ta logique)
                        cursor.execute(stmt, params) #le paramètre séléctionné sera mis a la place du %s de EXEC_ID dans la requête SQL 
                    
                    #! ---------------------------------------- #
                    else:
                        cursor.execute(stmt)
                    logger.info(f"Statement {i}/{len(statements)} exécuté avec succès.")
                except Exception as stmt_error:
                    logger.error(f"Erreur dans le statement {i}/{len(statements)} : {stmt_error}")
                    raise
        logger.info(f"Script terminé avec succès : {file_path}")
    except Exception as e:
        logger.error(f"Échec du script {file_path} avec erreur : {e}")
        raise


# SQL pour insérer des données dans la table R_ROOM à partir de la table CHAMBRE

INSERT INTO BASE_WORK.PUBLIC.R_ROOM (
    ROOM_NUM,
    ROOM_NAME,
    FLOR_NUM,
    BULD_NAME,
    ROOM_TYP,
    ROOM_DAY_RATE,
    CRTN_DT,
    EXEC_ID,
    EXEC_DATE,
    EXEC_USER
)
SELECT
    NO_CHAMBRE AS ROOM_NUM,
    NOM_CHAMBRE AS ROOM_NAME,
    NO_ETAGE AS FLOR_NUM,
    NOM_BATIMENT AS BULD_NAME,
    TYPE_CHAMBRE AS ROOM_TYP,
    PRIX_JOUR AS ROOM_DAY_RATE,
    DT_CREATION AS CRTN_DT,
    %s AS EXEC_ID,
    %s AS EXEC_DATE,
    %s AS EXEC_USER
FROM BASE_STAGING.PUBLIC.CHAMBRE;

#on peut aussi mettre plusieurs paramètres dans le tuple params, par exemple :
# params = ('EXEC_ID_VALUE', 'EXEC_DATE_VALUE', 'EXEC_USER_VALUE')
# attention dans ce cas il faut bien respecter l'ordre des paramètres dans la requête SQL

