create TABLE seats.COUNTRY (
	CO_ID bigint PRIMARY KEY, 
	CO_NAME text, 
	CO_CODE_2 text, 
	CO_CODE_3 text);
COPY seats.country  (CO_ID,CO_NAME,CO_CODE_2,CO_CODE_3) from '/home/ubuntu/seats/country.csv' WITH DELIMITER = ',' AND HEADER = true  AND NULL = 'N';