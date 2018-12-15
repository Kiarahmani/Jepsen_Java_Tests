drop KEYSPACE IF EXISTS testks;

create KEYSPACE testks WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1':1,'DC2':1,'DC3':1};


