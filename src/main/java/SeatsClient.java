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

	public static int deposit_checking(CassandraConnection conn, int id, int bal) throws Exception {
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT balance FROM checking WHERE id = ? ");
			PreparedStatement stmt2 = conn.prepareStatement("SELECT balance FROM total WHERE id = ? ");
			stmt.setInt(1, id);
			stmt2.setInt(1, id);
			ResultSet results = stmt.executeQuery();
			ResultSet results2 = stmt2.executeQuery();
			int old_bal = results.getInt("balance");
			int old_bal2 = results2.getInt("balance");
			stmt = conn.prepareStatement("UPDATE checking SET balance = ? WHERE id = ? ");
			stmt.setInt(1, old_bal + bal);
			stmt.setInt(2, id);
			stmt.executeUpdate();

			stmt2 = conn.prepareStatement("UPDATE total SET balance = ? WHERE id = ? ");
			stmt2.setInt(1, old_bal2 + bal);
			stmt2.setInt(2, id);
			stmt2.executeUpdate();

			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	public static int deposit_saving(CassandraConnection conn, int id, int bal) throws Exception {
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT balance FROM saving WHERE id = ? ");
			PreparedStatement stmt2 = conn.prepareStatement("SELECT balance FROM total WHERE id = ? ");
			stmt.setInt(1, id);
			stmt2.setInt(1, id);
			ResultSet results = stmt.executeQuery();
			ResultSet results2 = stmt2.executeQuery();
			int old_bal = results.getInt("balance");
			int old_bal2 = results2.getInt("balance");
			stmt = conn.prepareStatement("UPDATE saving SET balance = ? WHERE id = ? ");
			stmt.setInt(1, old_bal + bal);
			stmt.setInt(2, id);
			stmt.executeUpdate();

			stmt2 = conn.prepareStatement("UPDATE total SET balance = ? WHERE id = ? ");
			stmt2.setInt(1, old_bal2 + bal);
			stmt2.setInt(2, id);
			stmt2.executeUpdate();

			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
 
	public static int check(CassandraConnection conn, int id) throws Exception {
		try {
			PreparedStatement stmt1 = conn.prepareStatement("SELECT balance FROM saving WHERE id = ? ");
			PreparedStatement stmt2 = conn.prepareStatement("SELECT balance FROM checking WHERE id = ? ");
			PreparedStatement stmt3 = conn.prepareStatement("SELECT balance FROM total WHERE id = ? ");
			stmt1.setInt(1, id);
			stmt2.setInt(1, id);
			stmt3.setInt(1, id);
			ResultSet results1 = stmt1.executeQuery();
			ResultSet results2 = stmt2.executeQuery();
			ResultSet results3 = stmt3.executeQuery();
			int saving_bal = results1.getInt("balance");
			int checking_bal = results2.getInt("balance");
			int total_bal = results3.getInt("balance");
			if (total_bal != (saving_bal + checking_bal))
				return 1;
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
