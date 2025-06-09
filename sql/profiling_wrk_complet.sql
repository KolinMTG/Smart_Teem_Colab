
-- ========================================
-- PROFILING COMPLET - WRK
-- min / max / avg / nulls
-- ========================================


-- R_PART
SELECT
    COUNT(*) AS total_rows,

    -- PART_ID
    COUNT(PART_ID) AS PART_ID_non_nulls,
    COUNT(*) - COUNT(PART_ID) AS PART_ID_nulls,
    MIN(PART_ID) AS PART_ID_min,
    MAX(PART_ID) AS PART_ID_max,
    AVG(PART_ID) AS PART_ID_avg,

    -- SRC_ID
    COUNT(SRC_ID) AS SRC_ID_non_nulls,
    COUNT(*) - COUNT(SRC_ID) AS SRC_ID_nulls,
    MIN(SRC_ID) AS SRC_ID_min,
    MAX(SRC_ID) AS SRC_ID_max,
    AVG(SRC_ID) AS SRC_ID_avg,

    -- SRC_TYP
    COUNT(SRC_TYP) AS SRC_TYP_non_nulls,
    COUNT(*) - COUNT(SRC_TYP) AS SRC_TYP_nulls

FROM BASE_WORK.PUBLIC.R_PART;

-- ========================================

-- R_ROOM
SELECT
    COUNT(*) AS total_rows,

    COUNT(ROOM_NUM) AS ROOM_NUM_non_nulls,
    COUNT(*) - COUNT(ROOM_NUM) AS ROOM_NUM_nulls,

    COUNT(ROOM_NAME) AS ROOM_NAME_non_nulls,
    COUNT(*) - COUNT(ROOM_NAME) AS ROOM_NAME_nulls,

    COUNT(FLOR_NUM) AS FLOR_NUM_non_nulls,
    COUNT(*) - COUNT(FLOR_NUM) AS FLOR_NUM_nulls,
    MIN(FLOR_NUM) AS FLOR_NUM_min,
    MAX(FLOR_NUM) AS FLOR_NUM_max,
    AVG(FLOR_NUM) AS FLOR_NUM_avg,

    COUNT(BULD_NAME) AS BULD_NAME_non_nulls,
    COUNT(*) - COUNT(BULD_NAME) AS BULD_NAME_nulls,

    COUNT(ROOM_TYP) AS ROOM_TYP_non_nulls,
    COUNT(*) - COUNT(ROOM_TYP) AS ROOM_TYP_nulls,

    COUNT(ROOM_DAY_RATE) AS ROOM_DAY_RATE_non_nulls,
    COUNT(*) - COUNT(ROOM_DAY_RATE) AS ROOM_DAY_RATE_nulls,
    MIN(ROOM_DAY_RATE) AS ROOM_DAY_RATE_min,
    MAX(ROOM_DAY_RATE) AS ROOM_DAY_RATE_max,
    AVG(ROOM_DAY_RATE) AS ROOM_DAY_RATE_avg,

    COUNT(CRTN_DT) AS CRTN_DT_non_nulls,
    COUNT(*) - COUNT(CRTN_DT) AS CRTN_DT_nulls,
    MIN(CRTN_DT) AS CRTN_DT_min,
    MAX(CRTN_DT) AS CRTN_DT_max,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls,
    MIN(EXEC_ID) AS EXEC_ID_min,
    MAX(EXEC_ID) AS EXEC_ID_max,
    AVG(EXEC_ID) AS EXEC_ID_avg

FROM BASE_WORK.PUBLIC.R_ROOM;

-- ========================================

-- Ajout des autres tables (je vais continuer pour toutes les 10 tables dans ce format)


-- ========================================

-- R_MEDC
SELECT
    COUNT(*) AS total_rows,

    COUNT(MEDC_ID) AS MEDC_ID_non_nulls,
    COUNT(*) - COUNT(MEDC_ID) AS MEDC_ID_nulls,
    MIN(MEDC_ID) AS MEDC_ID_min,
    MAX(MEDC_ID) AS MEDC_ID_max,
    AVG(MEDC_ID) AS MEDC_ID_avg,

    COUNT(MEDC_CD) AS MEDC_CD_non_nulls,
    COUNT(*) - COUNT(MEDC_CD) AS MEDC_CD_nulls,

    COUNT(MEDC_NAME) AS MEDC_NAME_non_nulls,
    COUNT(*) - COUNT(MEDC_NAME) AS MEDC_NAME_nulls,

    COUNT(MEDC_COND) AS MEDC_COND_non_nulls,
    COUNT(*) - COUNT(MEDC_COND) AS MEDC_COND_nulls,

    COUNT(MEDC_CATG) AS MEDC_CATG_non_nulls,
    COUNT(*) - COUNT(MEDC_CATG) AS MEDC_CATG_nulls,

    COUNT(MANF_BRND) AS MANF_BRND_non_nulls,
    COUNT(*) - COUNT(MANF_BRND) AS MANF_BRND_nulls,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls,
    MIN(EXEC_ID) AS EXEC_ID_min,
    MAX(EXEC_ID) AS EXEC_ID_max,
    AVG(EXEC_ID) AS EXEC_ID_avg

FROM BASE_WORK.PUBLIC.R_MEDC;

-- ========================================

-- O_INDV
SELECT
    COUNT(*) AS total_rows,

    COUNT(PART_ID) AS PART_ID_non_nulls,
    COUNT(*) - COUNT(PART_ID) AS PART_ID_nulls,

    COUNT(INDV_NAME) AS INDV_NAME_non_nulls,
    COUNT(*) - COUNT(INDV_NAME) AS INDV_NAME_nulls,

    COUNT(INDV_FIRS_NAME) AS INDV_FIRS_NAME_non_nulls,
    COUNT(*) - COUNT(INDV_FIRS_NAME) AS INDV_FIRS_NAME_nulls,

    COUNT(INDV_STTS_CD) AS INDV_STTS_CD_non_nulls,
    COUNT(*) - COUNT(INDV_STTS_CD) AS INDV_STTS_CD_nulls,

    COUNT(CRTN_DTTM) AS CRTN_DTTM_non_nulls,
    COUNT(*) - COUNT(CRTN_DTTM) AS CRTN_DTTM_nulls,
    MIN(CRTN_DTTM) AS CRTN_DTTM_min,
    MAX(CRTN_DTTM) AS CRTN_DTTM_max,

    COUNT(UPDT_DTTM) AS UPDT_DTTM_non_nulls,
    COUNT(*) - COUNT(UPDT_DTTM) AS UPDT_DTTM_nulls,
    MIN(UPDT_DTTM) AS UPDT_DTTM_min,
    MAX(UPDT_DTTM) AS UPDT_DTTM_max,

    COUNT(BIRT_DT) AS BIRT_DT_non_nulls,
    COUNT(*) - COUNT(BIRT_DT) AS BIRT_DT_nulls,
    MIN(BIRT_DT) AS BIRT_DT_min,
    MAX(BIRT_DT) AS BIRT_DT_max,

    COUNT(BIRT_CITY) AS BIRT_CITY_non_nulls,
    COUNT(*) - COUNT(BIRT_CITY) AS BIRT_CITY_nulls,

    COUNT(BIRT_CNTR) AS BIRT_CNTR_non_nulls,
    COUNT(*) - COUNT(BIRT_CNTR) AS BIRT_CNTR_nulls,

    COUNT(SOCL_NUM) AS SOCL_NUM_non_nulls,
    COUNT(*) - COUNT(SOCL_NUM) AS SOCL_NUM_nulls,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls

FROM BASE_WORK.PUBLIC.O_INDV;

-- (les autres blocs suivront pour les 5 tables restantes)

-- ========================================

-- O_STFF
SELECT
    COUNT(*) AS total_rows,

    COUNT(PART_ID) AS PART_ID_non_nulls,
    COUNT(*) - COUNT(PART_ID) AS PART_ID_nulls,

    COUNT(WORK_STRT_DTTM) AS WORK_STRT_DTTM_non_nulls,
    COUNT(*) - COUNT(WORK_STRT_DTTM) AS WORK_STRT_DTTM_nulls,
    MIN(WORK_STRT_DTTM) AS WORK_STRT_DTTM_min,
    MAX(WORK_STRT_DTTM) AS WORK_STRT_DTTM_max,

    COUNT(WORK_END_DTTM) AS WORK_END_DTTM_non_nulls,
    COUNT(*) - COUNT(WORK_END_DTTM) AS WORK_END_DTTM_nulls,
    MIN(WORK_END_DTTM) AS WORK_END_DTTM_min,
    MAX(WORK_END_DTTM) AS WORK_END_DTTM_max,

    COUNT(WORK_END_RESN) AS WORK_END_RESN_non_nulls,
    COUNT(*) - COUNT(WORK_END_RESN) AS WORK_END_RESN_nulls,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls

FROM BASE_WORK.PUBLIC.O_STFF;

-- ========================================

-- O_TELP
SELECT
    COUNT(*) AS total_rows,

    COUNT(PART_ID) AS PART_ID_non_nulls,
    COUNT(*) - COUNT(PART_ID) AS PART_ID_nulls,

    COUNT(CNTR_IND) AS CNTR_IND_non_nulls,
    COUNT(*) - COUNT(CNTR_IND) AS CNTR_IND_nulls,

    COUNT(TELP_NUM) AS TELP_NUM_non_nulls,
    COUNT(*) - COUNT(TELP_NUM) AS TELP_NUM_nulls,

    COUNT(STRT_VALD_DTTM) AS STRT_VALD_DTTM_non_nulls,
    COUNT(*) - COUNT(STRT_VALD_DTTM) AS STRT_VALD_DTTM_nulls,
    MIN(STRT_VALD_DTTM) AS STRT_VALD_DTTM_min,
    MAX(STRT_VALD_DTTM) AS STRT_VALD_DTTM_max,

    COUNT(END_VALD_DTTM) AS END_VALD_DTTM_non_nulls,
    COUNT(*) - COUNT(END_VALD_DTTM) AS END_VALD_DTTM_nulls,
    MIN(END_VALD_DTTM) AS END_VALD_DTTM_min,
    MAX(END_VALD_DTTM) AS END_VALD_DTTM_max,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls

FROM BASE_WORK.PUBLIC.O_TELP;

-- ========================================

-- O_ADDR
SELECT
    COUNT(*) AS total_rows,

    COUNT(PART_ID) AS PART_ID_non_nulls,
    COUNT(*) - COUNT(PART_ID) AS PART_ID_nulls,

    COUNT(STRT_NUM) AS STRT_NUM_non_nulls,
    COUNT(*) - COUNT(STRT_NUM) AS STRT_NUM_nulls,

    COUNT(STRT_DSC) AS STRT_DSC_non_nulls,
    COUNT(*) - COUNT(STRT_DSC) AS STRT_DSC_nulls,

    COUNT(COMP_STRT) AS COMP_STRT_non_nulls,
    COUNT(*) - COUNT(COMP_STRT) AS COMP_STRT_nulls,

    COUNT(POST_CD) AS POST_CD_non_nulls,
    COUNT(*) - COUNT(POST_CD) AS POST_CD_nulls,

    COUNT(CITY_NAME) AS CITY_NAME_non_nulls,
    COUNT(*) - COUNT(CITY_NAME) AS CITY_NAME_nulls,

    COUNT(CNTR_NAME) AS CNTR_NAME_non_nulls,
    COUNT(*) - COUNT(CNTR_NAME) AS CNTR_NAME_nulls,

    COUNT(STRT_VALD_DTTM) AS STRT_VALD_DTTM_non_nulls,
    COUNT(*) - COUNT(STRT_VALD_DTTM) AS STRT_VALD_DTTM_nulls,
    MIN(STRT_VALD_DTTM) AS STRT_VALD_DTTM_min,
    MAX(STRT_VALD_DTTM) AS STRT_VALD_DTTM_max,

    COUNT(END_VALD_DTTM) AS END_VALD_DTTM_non_nulls,
    COUNT(*) - COUNT(END_VALD_DTTM) AS END_VALD_DTTM_nulls,
    MIN(END_VALD_DTTM) AS END_VALD_DTTM_min,
    MAX(END_VALD_DTTM) AS END_VALD_DTTM_max,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls

FROM BASE_WORK.PUBLIC.O_ADDR;

-- ========================================

-- Je continue avec les 3 dernières : O_CONS, O_TRET, O_HOSP

-- ========================================

-- O_CONS
SELECT
    COUNT(*) AS total_rows,

    COUNT(CONS_ID) AS CONS_ID_non_nulls,
    COUNT(*) - COUNT(CONS_ID) AS CONS_ID_nulls,

    COUNT(STFF_ID) AS STFF_ID_non_nulls,
    COUNT(*) - COUNT(STFF_ID) AS STFF_ID_nulls,

    COUNT(PATN_ID) AS PATN_ID_non_nulls,
    COUNT(*) - COUNT(PATN_ID) AS PATN_ID_nulls,

    COUNT(TRET_ID) AS TRET_ID_non_nulls,
    COUNT(*) - COUNT(TRET_ID) AS TRET_ID_nulls,

    COUNT(CONS_STRT_DTTM) AS CONS_STRT_DTTM_non_nulls,
    COUNT(*) - COUNT(CONS_STRT_DTTM) AS CONS_STRT_DTTM_nulls,
    MIN(CONS_STRT_DTTM) AS CONS_STRT_DTTM_min,
    MAX(CONS_STRT_DTTM) AS CONS_STRT_DTTM_max,

    COUNT(CONS_END_DTTM) AS CONS_END_DTTM_non_nulls,
    COUNT(*) - COUNT(CONS_END_DTTM) AS CONS_END_DTTM_nulls,
    MIN(CONS_END_DTTM) AS CONS_END_DTTM_min,
    MAX(CONS_END_DTTM) AS CONS_END_DTTM_max,

    COUNT(PATN_WEGH) AS PATN_WEGH_non_nulls,
    COUNT(*) - COUNT(PATN_WEGH) AS PATN_WEGH_nulls,
    MIN(PATN_WEGH) AS PATN_WEGH_min,
    MAX(PATN_WEGH) AS PATN_WEGH_max,
    AVG(PATN_WEGH) AS PATN_WEGH_avg,

    COUNT(PATN_TEMP) AS PATN_TEMP_non_nulls,
    COUNT(*) - COUNT(PATN_TEMP) AS PATN_TEMP_nulls,
    MIN(PATN_TEMP) AS PATN_TEMP_min,
    MAX(PATN_TEMP) AS PATN_TEMP_max,
    AVG(PATN_TEMP) AS PATN_TEMP_avg,

    COUNT(TEMP_UNIT) AS TEMP_UNIT_non_nulls,
    COUNT(*) - COUNT(TEMP_UNIT) AS TEMP_UNIT_nulls,

    COUNT(BLD_PRSS) AS BLD_PRSS_non_nulls,
    COUNT(*) - COUNT(BLD_PRSS) AS BLD_PRSS_nulls,
    MIN(BLD_PRSS) AS BLD_PRSS_min,
    MAX(BLD_PRSS) AS BLD_PRSS_max,
    AVG(BLD_PRSS) AS BLD_PRSS_avg,

    COUNT(PATH_DSC) AS PATH_DSC_non_nulls,
    COUNT(*) - COUNT(PATH_DSC) AS PATH_DSC_nulls,

    COUNT(DIBT_IND) AS DIBT_IND_non_nulls,
    COUNT(*) - COUNT(DIBT_IND) AS DIBT_IND_nulls,

    COUNT(HOSP_IND) AS HOSP_IND_non_nulls,
    COUNT(*) - COUNT(HOSP_IND) AS HOSP_IND_nulls,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls

FROM BASE_WORK.PUBLIC.O_CONS;

-- ========================================

-- O_TRET
SELECT
    COUNT(*) AS total_rows,

    COUNT(TRET_ID) AS TRET_ID_non_nulls,
    COUNT(*) - COUNT(TRET_ID) AS TRET_ID_nulls,

    COUNT(MEDC_ID) AS MEDC_ID_non_nulls,
    COUNT(*) - COUNT(MEDC_ID) AS MEDC_ID_nulls,

    COUNT(MEDC_QTY) AS MEDC_QTY_non_nulls,
    COUNT(*) - COUNT(MEDC_QTY) AS MEDC_QTY_nulls,
    MIN(MEDC_QTY) AS MEDC_QTY_min,
    MAX(MEDC_QTY) AS MEDC_QTY_max,
    AVG(MEDC_QTY) AS MEDC_QTY_avg,

    COUNT(DOSG_DSC) AS DOSG_DSC_non_nulls,
    COUNT(*) - COUNT(DOSG_DSC) AS DOSG_DSC_nulls,

    COUNT(CONS_ID) AS CONS_ID_non_nulls,
    COUNT(*) - COUNT(CONS_ID) AS CONS_ID_nulls,

    COUNT(TRET_CRTN_DTTM) AS TRET_CRTN_DTTM_non_nulls,
    COUNT(*) - COUNT(TRET_CRTN_DTTM) AS TRET_CRTN_DTTM_nulls,
    MIN(TRET_CRTN_DTTM) AS TRET_CRTN_DTTM_min,
    MAX(TRET_CRTN_DTTM) AS TRET_CRTN_DTTM_max,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls

FROM BASE_WORK.PUBLIC.O_TRET;

-- ========================================

-- O_HOSP
SELECT
    COUNT(*) AS total_rows,

    COUNT(HOSP_ID) AS HOSP_ID_non_nulls,
    COUNT(*) - COUNT(HOSP_ID) AS HOSP_ID_nulls,

    COUNT(CONS_ID) AS CONS_ID_non_nulls,
    COUNT(*) - COUNT(CONS_ID) AS CONS_ID_nulls,

    COUNT(ROOM_NUM) AS ROOM_NUM_non_nulls,
    COUNT(*) - COUNT(ROOM_NUM) AS ROOM_NUM_nulls,

    COUNT(HOSP_STRT_DTTM) AS HOSP_STRT_DTTM_non_nulls,
    COUNT(*) - COUNT(HOSP_STRT_DTTM) AS HOSP_STRT_DTTM_nulls,
    MIN(HOSP_STRT_DTTM) AS HOSP_STRT_DTTM_min,
    MAX(HOSP_STRT_DTTM) AS HOSP_STRT_DTTM_max,

    COUNT(HOSP_END_DTTM) AS HOSP_END_DTTM_non_nulls,
    COUNT(*) - COUNT(HOSP_END_DTTM) AS HOSP_END_DTTM_nulls,
    MIN(HOSP_END_DTTM) AS HOSP_END_DTTM_min,
    MAX(HOSP_END_DTTM) AS HOSP_END_DTTM_max,

    COUNT(HOSP_FINL_RATE) AS HOSP_FINL_RATE_non_nulls,
    COUNT(*) - COUNT(HOSP_FINL_RATE) AS HOSP_FINL_RATE_nulls,
    MIN(HOSP_FINL_RATE) AS HOSP_FINL_RATE_min,
    MAX(HOSP_FINL_RATE) AS HOSP_FINL_RATE_max,
    AVG(HOSP_FINL_RATE) AS HOSP_FINL_RATE_avg,

    COUNT(STFF_ID) AS STFF_ID_non_nulls,
    COUNT(*) - COUNT(STFF_ID) AS STFF_ID_nulls,

    COUNT(EXEC_ID) AS EXEC_ID_non_nulls,
    COUNT(*) - COUNT(EXEC_ID) AS EXEC_ID_nulls

FROM BASE_WORK.PUBLIC.O_HOSP

-- ========================================
