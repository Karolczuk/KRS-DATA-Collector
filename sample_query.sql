/*
  EXAMPLE SQL QUERIES
*/

--How many records exist in S and P table - Companies

SELECT COUNT(*)
FROM
    (SELECT * FROM firmy_krs_table_P fktp
    UNION ALL
    SELECT * FROM firmy_krs_table_S fkts) firmy_krs_table_P_and_S

--On what days were the updates of a given KRS?

SELECT fkthc.STAN_Z_DNIA 
FROM firmy_krs_table_history_change fkthc
WHERE fkthc.KRS_CHANGED LIKE  '%231271%'

--List of emails to companies, e.g. for a mailing campaign

SELECT fktp.KRS, fktp.NAZWA, fktp.NIP, fktp.REGON, fktp.EMAIL
FROM firmy_krs_table_P fktp
WHERE fktp.EMAIL IS NOT NULL AND (fktp.PKD_GLOWNY IN ('71.11.Z','71.12.Z','41.20.Z')
OR fktp.PKD_POZOSTALE LIKE '%71.11.Z%'
OR fktp.PKD_POZOSTALE LIKE '%71.12.Z%'
OR fktp.PKD_POZOSTALE LIKE '%41.20.Z%')

--List of website address of foundation

SELECT fkts.KRS, fkts.NAZWA, fkts.FORMA_PRAWNA, fkts.NIP, fkts.REGON, fkts.WWW 
FROM firmy_krs_table_S fkts 
WHERE fkts.WWW IS NOT NULL AND fkts.FORMA_PRAWNA = 'FUNDACJA'
UNION 
SELECT fktp.KRS, fktp.NAZWA, fktp.FORMA_PRAWNA, fktp.NIP, fktp.REGON, fktp.WWW  
FROM firmy_krs_table_P fktp 
WHERE fktp.WWW IS NOT NULL AND fktp.FORMA_PRAWNA = 'FUNDACJA' 
AND fktp.KRS NOT IN (SELECT fkts.KRS 
					FROM firmy_krs_table_S fkts 
					WHERE fkts.WWW IS NOT NULL AND fkts.FORMA_PRAWNA = 'FUNDACJA')	

--Number of companies in every voivodeship

SELECT CASE 
           WHEN WOJEWODZTWO IS NULL OR WOJEWODZTWO = '-' OR WOJEWODZTWO = '-----' 
           THEN 'BRAK' 
           ELSE WOJEWODZTWO 
       END AS WOJEWODZTWO,
       FORMA_PRAWNA,
       COUNT(NAZWA) AS LICZBA
FROM firmy_krs_table_P fktp
GROUP BY CASE 
             WHEN WOJEWODZTWO IS NULL OR WOJEWODZTWO = '-' OR WOJEWODZTWO = '-----' 
             THEN 'BRAK' 
             ELSE WOJEWODZTWO 
         END, FORMA_PRAWNA
ORDER BY WOJEWODZTWO, FORMA_PRAWNA

--The largest share capital in euro

SET @kurs = 4.32;

SELECT
KRS,
NAZWA,
CASE 
	WHEN WALUTA_KAPITALU_ZAKLADOWEGO = 'PLN'
	THEN ROUND(WYSOKOSC_KAPITALU_ZAKLADOWEGO / @kurs, 2)
	ELSE ROUND(WYSOKOSC_KAPITALU_ZAKLADOWEGO, 2)
END AS WYSOKOSC_KAPITALU_ZAKLADOWEGO_W_EURO,
CASE 
	WHEN WALUTA_KAPITALU_ZAKLADOWEGO = 'PLN'
	THEN 'EUR'
	ELSE WALUTA_KAPITALU_ZAKLADOWEGO
END AS WALUTA_KAPITALU_ZAKLADOWEGO
FROM firmy_krs_table_P fktp
WHERE WYSOKOSC_KAPITALU_ZAKLADOWEGO IS NOT NULL
ORDER BY WYSOKOSC_KAPITALU_ZAKLADOWEGO_W_EURO DESC
limit 100

--Number of companies that are associations and enterprises

SELECT fktp.KRS, fktp.NAZWA, fktp.REJESTR, fkts.REJESTR, fktp.FORMA_PRAWNA 
FROM firmy_krs_table_P fktp
JOIN firmy_krs_table_S fkts ON fktp.KRS = fkts.KRS 
