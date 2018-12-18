drop KEYSPACE IF EXISTS seats;

create KEYSPACE seats WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_n1':1,'dc_n2':1,'dc_n3':1};

create TABLE seats.COUNTRY (CO_ID int PRIMARY KEY , CO_NAME text, CO_CODE_2 text, CO_CODE_3 text);

create TABLE seats.AIRPORT (AP_ID int PRIMARY KEY, AP_CODE text, AP_NAME text, AP_CITY text, AP_POSTAL_CODE text, AP_CO_ID int, AP_LONGITUDE int, AP_LATITUDE int,
AP_GMT_OFFSET int, AP_WAC int, AP_IATTR00 int, AP_IATTR01 int, AP_IATTR02 int, AP_IATTR03 int, AP_IATTR04 int, AP_IATTR05 int, AP_IATTR06 int, 
AP_IATTR07 int, AP_IATTR08 int, AP_IATTR09 int, AP_IATTR10 int, AP_IATTR11 int, AP_IATTR12 int, AP_IATTR13 int,AP_IATTR14 int, AP_IATTR15 int);


create TABLE seats.AIRPORT_DISTANCE (D_AP_ID0 int, D_AP_ID1 int, D_DISTANCE int , PRIMARY KEY (D_AP_ID0, D_AP_ID1));