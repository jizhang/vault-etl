DROP TABLE IF EXISTS zj_tmp1;
CREATE TABLE zj_tmp1 AS SELECT COUNT(*) AS cnt FROM ds_crius;

REPLACE INTO dw_crius_daily (
    report_date
    ,cnt
)
SELECT
    {date}
    ,cnt
FROM zj_tmp1;
