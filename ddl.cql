drop KEYSPACE IF EXISTS testks;

create KEYSPACE testks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_n1':1,'dc_n2':1,'dc_n3':1};

create TABLE testks.a (id int PRIMARY KEY , balance int);
create TABLE testks.b (id int PRIMARY KEY , balance int);

