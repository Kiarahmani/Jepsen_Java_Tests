import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class SeatsClient {

	private static boolean _NO_ERROR_MODE = true;
	private static boolean _SHOW_CQL_MESSAGES = false;
	private static int _NUMBER_OF_CONNECTIONS_PER_NODE = 1;
	private static RoundRobin<CassandraConnection> connectionPool = new RoundRobin<CassandraConnection>();
	private static Iterator<CassandraConnection> connections = connectionPool.iterator();

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
		return connect;
	}

	public static void closeConnection(Connection connection) {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/*
	 * 
	 * (1) DEPOSIT
	 * 
	 */

	public static int deposit(CassandraConnection conn, int id, int bal) throws Exception {
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT balance FROM checking WHERE id = ? ");
			;
			stmt.setInt(1, id);
			ResultSet results = stmt.executeQuery();
			int old_bal = results.getInt("balance");
			stmt = conn.prepareStatement("UPDATE checking SET balance = ? WHERE id = ? ");
			stmt.setInt(1, old_bal + bal);
			stmt.setInt(2, id);
			stmt.executeUpdate();

			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
}
