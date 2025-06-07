
-- Ce script insère des données dans la table R_PART à partir de la table PERSONNEL et PATIENT de BASE_STAGING.
WITH combinaison_unique AS ( -- crée une table temporaire contenant les combinaisons uniques de (ID_PERSONNEL, FONCTION_PERSONNEL)
    SELECT 
        ID_PERSONNEL,
        FONCTION_PERSONNEL,
        ID_PATIENT,
        DENSE_RANK() OVER ( -- Utilise DENSE_RANK pour attribuer un identifiant incrémentale unique à chaque combinaison de (ID_PERSONNEL, FONCTION_PERSONNEL)
            ORDER BY ID_PERSONNEL, FONCTION_PERSONNEL
        ) AS PART_ID
    FROM BASE_STAGING.PERSONNEL
)
INSERT INTO BASE_WORK.R_PART (
    PART_ID,
    SRC_ID,
    SRC_TYP,
    SRC_PATIENT
)
SELECT
    PART_ID,
    ID_PERSONNEL AS SRC_ID,
    FONCTION_PERSONNEL AS SRC_TYP,
    ID_PATIENT AS SRC_PATIENT
FROM combinaison_unique; -- On insère les données de notre table temporaire dans la table R_PART


-- Même logique pour la table PATIENT, mais sans la colonne FONCTION, remplacé donc par la valeur 'Patient' pour SRC_TYP
WITH combinaison_patient AS (
    SELECT
        ID_PATIENT,
        DENSE_RANK() OVER (
            ORDER BY ID_PATIENT
        ) AS PART_ID
    FROM BASE_STAGING.PATIENT
)
INSERT INTO BASE_WORK.R_PART (
    PART_ID,
    SRC_ID,
    SRC_TYP
)
SELECT
    PART_ID,
    ID_PATIENT AS SRC_ID,
    'Patient' AS SRC_TYP
FROM combinaison_patient;