CREATE OR REPLACE VIEW BASE_SOCLE.PUBLIC.vw_medication_by_pathology AS
SELECT 
    c.PATH_DSC AS pathology,
    t.MEDC_ID AS medication_id,
    m.MEDC_NAME AS medication_name,
    DATE(t.TRET_CRTN_DTTM) AS prescription_date,
    SUM(t.MEDC_QTY) AS total_quantity
FROM
    BASE_SOCLE.PUBLIC.TREATMENT t
JOIN
    BASE_SOCLE.PUBLIC.CONSULTATION c ON t.CONS_ID = c.CONS_ID
JOIN
    BASE_SOCLE.PUBLIC.MEDICINE m ON t.MEDC_ID = m.MEDC_ID
GROUP BY
    c.PATH_DSC,
    t.MEDC_ID,
    m.MEDC_NAME,
    DATE(t.TRET_CRTN_DTTM);
