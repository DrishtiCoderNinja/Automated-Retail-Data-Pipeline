SELECT  DATE,STORE_ID, ROUND((SUM(SP) - SUM(CP)), 2) AS lc_profit 
FROM store.daily_transactions WHERE process_date = CURDATE() GROUP BY 1,2;