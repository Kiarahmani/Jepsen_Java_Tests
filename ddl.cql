drop KEYSPACE IF EXISTS seats;
create KEYSPACE seats WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_n1':1};