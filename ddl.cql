drop KEYSPACE IF EXISTS seats;

create KEYSPACE seats WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_n1':1,'dc_n2':1,'dc_n3':1};

create TABLE seats.COUNTRY (CO_ID int PRIMARY KEY , CO_NAME text, CO_CODE_2 text, CO_CODE_3 text);

create TABLE seats.AIRPORT (AP_ID int PRIMARY KEY, AP_CODE text, AP_NAME text, AP_CITY text, AP_POSTAL_CODE text, AP_CO_ID int, AP_LONGITUDE int, AP_LATITUDE int,
AP_GMT_OFFSET int, AP_WAC int, AP_IATTR00 int, AP_IATTR01 int, AP_IATTR02 int, AP_IATTR03 int, AP_IATTR04 int, AP_IATTR05 int, AP_IATTR06 int, 
AP_IATTR07 int, AP_IATTR08 int, AP_IATTR09 int, AP_IATTR10 int, AP_IATTR11 int, AP_IATTR12 int, AP_IATTR13 int,AP_IATTR14 int, AP_IATTR15 int);


create TABLE seats.AIRPORT_DISTANCE (D_AP_ID0 int, D_AP_ID1 int, D_DISTANCE int , PRIMARY KEY (D_AP_ID0, D_AP_ID1));

create TABLE seats.AIRLINE ( 
	AL_ID int PRIMARY KEY,
    AL_IATA_CODE text,
    AL_ICAO_CODE text,
    AL_CALL_SIGN text,
    AL_NAME text,
    AL_CO_ID int,
    AL_IATTR00 int,
    AL_IATTR01 int,
    AL_IATTR02 int,
    AL_IATTR03 int,
    AL_IATTR04 int,
    AL_IATTR05 int,
    AL_IATTR06 int,
    AL_IATTR07 int,
    AL_IATTR08 int,
    AL_IATTR09 int,
    AL_IATTR10 int,
    AL_IATTR11 int,
    AL_IATTR12 int,
    AL_IATTR13 int,
    AL_IATTR14 int,
    AL_IATTR15 int);
    
    

create TABLE seats.CUSTOMER ( 
	C_ID           int PRIMARY KEY,
    C_ID_STR       text,
    C_BASE_AP_ID   int,
    C_BALANCE      int,
    C_SATTR00      text,
    C_SATTR01      text,
    C_SATTR02      text,
    C_SATTR03      text,
    C_SATTR04      text,
    C_SATTR05      text,
    C_SATTR06      text,
    C_SATTR07      text,
    C_SATTR08      text,
    C_SATTR09      text,
    C_SATTR10      text,
    C_SATTR11      text,
    C_SATTR12      text,
    C_SATTR13      text,
    C_SATTR14      text,
    C_SATTR15      text,
    C_SATTR16      text,
    C_SATTR17      text,
    C_SATTR18      text,
    C_SATTR19      text,
    C_IATTR00      int,
    C_IATTR01      int,
    C_IATTR02      int,
    C_IATTR03      int,
    C_IATTR04      int,
    C_IATTR05      int,
    C_IATTR06      int,
    C_IATTR07      int,
    C_IATTR08      int,
    C_IATTR09      int,
    C_IATTR10      int,
    C_IATTR11      int,
    C_IATTR12      int,
    C_IATTR13      int,
    C_IATTR14      int,
    C_IATTR15      int,
    C_IATTR16      int,
    C_IATTR17      int,
    C_IATTR18      int,
    C_IATTR19      int
);


create TABLE seats.FREQUENT_FLYER ( 
    FF_C_ID        int,
    FF_AL_ID       int,
    FF_C_ID_STR    text,
    FF_SATTR00     text,
    FF_SATTR01     text,
    FF_SATTR02     text,
    FF_SATTR03     text,
    FF_IATTR00     int,
    FF_IATTR01     int,
    FF_IATTR02     int,
    FF_IATTR03     int,
    FF_IATTR04     int,
    FF_IATTR05     int,
    FF_IATTR06     int,
    FF_IATTR07     int,
    FF_IATTR08     int,
    FF_IATTR09     int,
    FF_IATTR10     int,
    FF_IATTR11     int,
    FF_IATTR12     int,
    FF_IATTR13     int,
    FF_IATTR14     int,
    FF_IATTR15     int,
    PRIMARY KEY (FF_C_ID, FF_AL_ID)
);


create TABLE seats.FLIGHT ( 
    F_ID            int,
    F_AL_ID         int,
    F_DEPART_AP_ID  int,
    F_DEPART_TIME   int, 
    F_ARRIVE_AP_ID  int,
    F_ARRIVE_TIME   int,
    F_STATUS        int,
    F_BASE_PRICE    int,
    F_SEATS_TOTAL   int,
    F_SEATS_LEFT    int,
    F_IATTR00       int,
    F_IATTR01       int,
    F_IATTR02       int,
    F_IATTR03       int,
    F_IATTR04       int,
    F_IATTR05       int,
    F_IATTR06       int,
    F_IATTR07       int,
    F_IATTR08       int,
    F_IATTR09       int,
    F_IATTR10       int,
    F_IATTR11       int,
    F_IATTR12       int,
    F_IATTR13       int,
    F_IATTR14       int,
    F_IATTR15       int,
    F_IATTR16       int,
    F_IATTR17       int,
    F_IATTR18       int,
    F_IATTR19       int,
    F_IATTR20       int,
    F_IATTR21       int,
    F_IATTR22       int,
    F_IATTR23       int,
    F_IATTR24       int,
    F_IATTR25       int,
    F_IATTR26       int,
    F_IATTR27       int,
    F_IATTR28       int,
    F_IATTR29       int,
    PRIMARY KEY (F_ID));


create TABLE seats.RESERVATION ( 
	 R_ID           int,
    R_C_ID          int,
    R_F_ID          int,
    R_SEAT          int,
    R_PRICE         double,
    R_IATTR00       int,
    R_IATTR01       int,
    R_IATTR02       int,
    R_IATTR03       int,
    R_IATTR04       int,
    R_IATTR05       int,
    R_IATTR06       int,
    R_IATTR07       int,
    R_IATTR08       int,
    PRIMARY KEY (R_ID, R_C_ID, R_F_ID)
);	
































