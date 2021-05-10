SELECT CAST(UNITID AS INT64) AS unit_id, INSTNM, CIPDESC, CREDDESC, CAST(EARN_MDN_HI_1YR AS INT64) AS median_earnings, 
CAST(DEBT_ALL_STGP_EVAL_MDN AS INT64) AS median_debt
FROM `chris-data-299601.college_scorecard.programs` 
