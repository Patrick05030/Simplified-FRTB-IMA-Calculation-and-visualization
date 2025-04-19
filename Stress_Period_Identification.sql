CREATE DATABASE frtb
USE frtb





USE frtb;
SELECT COUNT(*) FROM df_10y_pl;
SELECT * FROM df_10y_pl LIMIT 30; -- 10Y scenario-PV data


-- calculate the 1-day P&L over 10Y PV data, using LAG() function.
-- This prepares us for Expected Shortfall and VaR calculations.
-- LAG() return the value from the previou row the default value is 1, PVt - PV(t-1) = daily_pl for each day
-- we found that the first row(the oldest day) is null value, cuz it don't have older data than it, so it don't have PV(t) for this date.


DROP TABLE daily_pl
CREATE TABLE daily_pl AS
SELECT 
    DATE,
    DESK3_A_TOTAL - LAG(DESK3_A_TOTAL) OVER (ORDER BY DATE) AS DESK3_A_PL,
    DESK3_B_TOTAL - LAG(DESK3_B_TOTAL) OVER (ORDER BY DATE) AS DESK3_B_PL,
    DESK3_C_TOTAL - LAG(DESK3_C_TOTAL) OVER (ORDER BY DATE) AS DESK3_C_PL,
    DESK3_D_TOTAL - LAG(DESK3_D_TOTAL) OVER (ORDER BY DATE) AS DESK3_D_PL,
    DESK3_E_TOTAL - LAG(DESK3_E_TOTAL) OVER (ORDER BY DATE) AS DESK3_E_PL,
    DESK3_F_TOTAL - LAG(DESK3_F_TOTAL) OVER (ORDER BY DATE) AS DESK3_F_PL,
    DESK3_G_TOTAL - LAG(DESK3_G_TOTAL) OVER (ORDER BY DATE) AS DESK3_G_PL,
    DESK3_H_TOTAL - LAG(DESK3_H_TOTAL) OVER (ORDER BY DATE) AS DESK3_H_PL,
    DESK3_I_TOTAL - LAG(DESK3_I_TOTAL) OVER (ORDER BY DATE) AS DESK3_I_PL,
    DESK3_J_TOTAL - LAG(DESK3_J_TOTAL) OVER (ORDER BY DATE) AS DESK3_J_PL,
    DESK3_K_TOTAL - LAG(DESK3_K_TOTAL) OVER (ORDER BY DATE) AS DESK3_K_PL,
    DESK3_L_TOTAL - LAG(DESK3_L_TOTAL) OVER (ORDER BY DATE) AS DESK3_L_PL,
    DESK3_M_TOTAL - LAG(DESK3_M_TOTAL) OVER (ORDER BY DATE) AS DESK3_M_PL,
    DESK3_N_TOTAL - LAG(DESK3_N_TOTAL) OVER (ORDER BY DATE) AS DESK3_N_PL
FROM df_10y_pl
ORDER BY DATE;

SELECT * FROM daily_pl limit 250 #already drop the first row of null value
SELECT COUNT(*) FROM daily_pl 

-- ------------------------Drop the first row which is the null value---------------------------------------------

SET SQL_SAFE_UPDATES = 0; #Disable Safe Update Mode Temporarily
SET SQL_SAFE_UPDATES = 1; #you can re-enable safe update mode 
DELETE FROM daily_pl -- WE drop the null from the daily_pl table, using where clause
WHERE DESK3_A_PL IS NULL;






CREATE OR REPLACE VIEW stress_period_desk_level AS
WITH numbered_pl AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY DATE) AS rn
    FROM daily_pl
),
rolling_250 AS (
    SELECT rn, DATE, 'DESK3_A' AS desk, SUM(DESK3_A_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS rolling_pl FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_B', SUM(DESK3_B_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_C', SUM(DESK3_C_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_D', SUM(DESK3_D_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_E', SUM(DESK3_E_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_F', SUM(DESK3_F_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_G', SUM(DESK3_G_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_H', SUM(DESK3_H_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_I', SUM(DESK3_I_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_J', SUM(DESK3_J_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_K', SUM(DESK3_K_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_L', SUM(DESK3_L_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_M', SUM(DESK3_M_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl UNION ALL
    SELECT rn, DATE, 'DESK3_N', SUM(DESK3_N_PL) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) FROM numbered_pl
),
ranked_rolling AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY desk ORDER BY rolling_pl ASC) AS desk_rank
    FROM rolling_250
    WHERE rn >= 250
),
start_date_cte AS (
    SELECT rn, DATE AS start_date FROM numbered_pl
)
SELECT 
    r.desk,
    s.start_date,
    r.DATE AS end_date,
    r.rolling_pl AS min_rolling_pl
FROM ranked_rolling r
JOIN start_date_cte s ON s.rn = r.rn - 249
WHERE r.desk_rank = 1
ORDER BY r.desk;

SELECT * FROM stress_period_desk_level;


DROP TABLE stress_period_report
-- -----------------stress_period_report has been combined desk and enterprise level Stressed Period and their rolling_PL----------------------------------------------
CREATE TABLE IF NOT EXISTS stress_period_report (
    desk VARCHAR(50),
    start_date DATE,
    end_date DATE,
    min_rolling_pl DECIMAL(18,2)
);
-- ----Insert from a view or visual table into a consolidated table(stress_period_report)-----
INSERT INTO stress_period_report (desk, start_date, end_date, min_rolling_pl)
SELECT * FROM stress_period_desk_level;





INSERT INTO stress_period_report (desk, start_date, end_date, min_rolling_pl)
WITH numbered_pl AS (
    SELECT *, ROW_NUMBER() OVER (ORDER BY DATE) AS rn
    FROM daily_pl
),
total_pl AS (
    SELECT 
        rn,
        DATE,
        COALESCE(DESK3_A_PL, 0) +
        COALESCE(DESK3_B_PL, 0) +
        COALESCE(DESK3_C_PL, 0) +
        COALESCE(DESK3_D_PL, 0) +
        COALESCE(DESK3_E_PL, 0) +
        COALESCE(DESK3_F_PL, 0) +
        COALESCE(DESK3_G_PL, 0) +
        COALESCE(DESK3_H_PL, 0) +
        COALESCE(DESK3_I_PL, 0) +
        COALESCE(DESK3_J_PL, 0) +
        COALESCE(DESK3_K_PL, 0) +
        COALESCE(DESK3_L_PL, 0) +
        COALESCE(DESK3_M_PL, 0) +
        COALESCE(DESK3_N_PL, 0) AS enterprise_pl
    FROM numbered_pl
),
rolling_enterprise AS (
    SELECT 
        rn,
        DATE,
        SUM(enterprise_pl) OVER (ORDER BY rn ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) AS rolling_250d_pl
    FROM total_pl
    WHERE rn >= 250
),
min_stress_period AS (
    SELECT *,
           ROW_NUMBER() OVER (ORDER BY rolling_250d_pl ASC) AS rnk
    FROM rolling_enterprise
)
SELECT 
    'ENTERPRISE' AS desk,
    start.DATE AS start_date,
    min.DATE AS end_date,
    min.rolling_250d_pl AS min_rolling_pl
FROM min_stress_period min
JOIN numbered_pl start ON start.rn = min.rn - 249
WHERE min.rnk = 1;

SELECT * FROM stress_period_report