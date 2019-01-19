package utils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class CassConn {
	private static RoundRobin<CassandraConnection> connectionPool = new RoundRobin<CassandraConnection>();
	private static Iterator<CassandraConnection> connections = connectionPool.iterator();
	private static int _NUMBER_OF_CONNECTIONS_PER_NODE = 90;

	public static void prepareConnections(int n, int c, String bench) {
		try {
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
			for (int i = 1; i <= n; i++)
				for (int j = 0; j < _NUMBER_OF_CONNECTIONS_PER_NODE; j++) {
					CassandraConnection connect = (CassandraConnection) DriverManager.getConnection("jdbc:cassandra://"
							+ "n" + String.valueOf(i) + ":9042/" + bench + "?"
							+ "consistency=ONE&retry=FallthroughRetryPolicy" + "&loadbalancing=RoundRobinPolicy()");
					connectionPool.add(connect);
				}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static CassandraConnection getConnection(String localAddr) {
		CassandraConnection connect = null;
		connect = connections.next();
		System.out.println("GET CONNECTION: " + connect.getClusterMetadata());
		return connect;
	}

	public static void closeConnection(CassandraConnection connection) {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
