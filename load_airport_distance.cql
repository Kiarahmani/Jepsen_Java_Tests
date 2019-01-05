create TABLE seats.AIRPORT_DISTANCE (
	D_AP_ID0 bigint, 
	D_AP_ID1 bigint,
	D_DISTANCE float, 
	PRIMARY KEY (D_AP_ID0, D_AP_ID1));
COPY seats.airport_distance (D_AP_ID0,D_AP_ID1,D_DISTANCE) from '/home/ubuntu/seats/airport_distance.csv' WITH DELIMITER = ',' AND HEADER = true  AND NULL = 'N';