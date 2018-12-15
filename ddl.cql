drop KEYSPACE IF EXISTS testks;

create KEYSPACE testks WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1':1,'DC2':1,'DC3':1};

create TABLE testks.a (id int PRIMARY KEY , balance int);

