drop KEYSPACE IF EXISTS seats;

create KEYSPACE seats WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_n1':1,'dc_n2':1,'dc_n3':1};

create TABLE seats.COUNTRY (CO_ID int PRIMARY KEY , CO_NAME text, CO_CODE_2 text, CO_CODE_3 text);


