INSERT INTO BASE_WORK.PUBLIC.R_PART (
    PART_ID,
    SRC_ID,
    SRC_TYP
)
SELECT *
FROM (
    WITH
    combinaison_personnel AS (
        SELECT 
            ID_PERSONNEL,
            FONCTION_PERSONNEL,
            DENSE_RANK() OVER (
                ORDER BY ID_PERSONNEL, FONCTION_PERSONNEL
            ) AS PART_ID
        FROM BASE_STAGING.PUBLIC.PERSONNEL
    ),
    combinaison_patient AS (
        SELECT 
            ID_PATIENT,
            DENSE_RANK() OVER (
                ORDER BY ID_PATIENT
            ) + (
                SELECT COALESCE(MAX(PART_ID), 0) FROM combinaison_personnel
            ) AS PART_ID
        FROM BASE_STAGING.PUBLIC.PATIENT
    )
    
    SELECT
        PART_ID,
        ID_PERSONNEL AS SRC_ID,
        FONCTION_PERSONNEL AS SRC_TYP
    FROM combinaison_personnel

    UNION ALL

    SELECT
        PART_ID,
        ID_PATIENT AS SRC_ID,
        'Patient' AS SRC_TYP
    FROM combinaison_patient
) AS full_data;
