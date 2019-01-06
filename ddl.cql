drop KEYSPACE IF EXISTS seats;
create KEYSPACE seats WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_n1':1,'dc_n2':1,'dc_n3':1,'dc_n4':1};