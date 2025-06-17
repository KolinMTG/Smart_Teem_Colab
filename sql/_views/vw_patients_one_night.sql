CREATE OR REPLACE VIEW BASE_SOCLE.PUBLIC.vw_hospital_stays AS
SELECT 
    c.CONS_ID,
    c.CONS_STRT_DTTM AS consultation_date,
    c.PATN_ID AS patient_id,
    h.HOSP_STRT_DTTM AS hosp_start_date,
    h.HOSP_END_DTTM AS hosp_end_date,
    CASE 
        WHEN h.HOSP_STRT_DTTM IS NOT NULL 
        AND DATEDIFF(day, h.HOSP_STRT_DTTM, h.HOSP_END_DTTM) >= 1 
        THEN TRUE
        ELSE FALSE
    END AS stayed_at_least_one_night
FROM BASE_SOCLE.PUBLIC.CONSULTATION c
LEFT JOIN BASE_SOCLE.PUBLIC.HOSPITALIZATION h 
    ON c.CONS_ID = h.CONS_ID;
