create TABLE seats.RESERVATION ( 
	R_ID           bigint,
    R_C_ID          bigint,
    R_F_ID          bigint,
    R_SEAT          bigint,
    R_PRICE         float,
    R_IATTR00       bigint,
    R_IATTR01       bigint,
    R_IATTR02       bigint,
    R_IATTR03       bigint,
    R_IATTR04       bigint,
    R_IATTR05       bigint,
    R_IATTR06       bigint,
    R_IATTR07       bigint,
    R_IATTR08       bigint,
    PRIMARY KEY (R_ID, R_C_ID, R_F_ID)
);	

CREATE INDEX resCustIDX on seats.reservation (R_C_ID);
CREATE INDEX resCFlghtIDX on seats.reservation (R_F_ID);

COPY seats.reservation  (R_ID, R_C_ID,  R_F_ID, R_SEAT, R_PRICE, R_IATTR00, R_IATTR01, R_IATTR02, R_IATTR03, R_IATTR04, R_IATTR05, R_IATTR06, R_IATTR07, R_IATTR08 ) from '/home/ubuntu/seats/reservation.csv' WITH DELIMITER = ',' AND HEADER = true  AND NULL = 'N';