CREATE VIEW positions (
 NAME,
 SYMBOL,
 INVESTMENT_TYPE,
 UNITS,
 FACE_VALUE,
 TOTAL_DOLLARS,
 ADD_DATE,
 END_DATE,
 TOTAL_COST,
 RETURN_RATE,
 RETURN_DOLLARS,
 IS_MATURED
)
AS
SELECT
  t1.NAME AS NAME,
  t1.SYMBOL AS SYMBOL,
  t1.INVESTMENT_TYPE AS INVESTMENT_TYPE,
  t1.UNITS AS UNITS,
  t1.FACE_VALUE AS FACE_VALUE,
  t1.TOTAL_DOLLARS AS TOTAL_DOLLARS,
  t1.ADD_DATE AS ADD_DATE,
  t1.END_DATE AS END_DATE,
  t1.TOTAL_COST AS TOTAL_COST,
  CASE WHEN t1.YTM = 0.0 THEN t1.APR ELSE t1.YTM END AS RETURN_RATE,
  CASE WHEN t1.YTM = 0.0 THEN t1.TOTAL_COST*t1.APR ELSE t1.TOTAL_COST*t1.YTM END AS RETURN_DOLLARS,
  CASE
    WHEN CAST(SUBSTR(t1.END_DATE,1,4) AS INTEGER) < CAST(STRFTIME('%Y',DATE('now')) AS INTEGER) THEN 1
    WHEN CAST(SUBSTR(t1.END_DATE,1,4) AS INTEGER) = CAST(STRFTIME('%Y',DATE('now')) AS INTEGER)
      AND CAST(SUBSTR(t1.END_DATE,6,2) AS INTEGER) < CAST(STRFTIME('%m',DATE('now')) AS INTEGER) THEN 1
    WHEN CAST(SUBSTR(t1.END_DATE,1,4) AS INTEGER) = CAST(STRFTIME('%Y',DATE('now')) AS INTEGER)
      AND CAST(SUBSTR(t1.END_DATE,6,2) AS INTEGER) = CAST(STRFTIME('%m',DATE('now')) AS INTEGER)
      AND CAST(SUBSTR(t1.END_DATE,9,2) AS INTEGER) < CAST(STRFTIME('%d',DATE('now')) AS INTEGER) THEN 1
    ELSE 0 END AS IS_MATURED
FROM transactions t1
ORDER BY IS_MATURED;
