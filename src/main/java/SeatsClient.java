import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SeatsClient {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

	public static Connection getConnection(String localAddr) {
		Connection connect = null;
		// "172.18.0.7"
		try {
			connect = DriverManager.getConnection("jdbc:cassandra://" + localAddr + ":9042/testks");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return connect;
	}

	public static void closeConnection(Connection connection) {
		try {
			connection.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static int decTransaction(Connection connect, int key, int value) throws Exception {
		try {
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");

			PreparedStatement preparedStatement = connect.prepareStatement("select * from A where id=?");
			preparedStatement.setInt(1, key);
			ResultSet rs = preparedStatement.executeQuery();
			int oldBal = -10000;
			if (rs.next())
				oldBal = rs.getInt("balance");
			
			Thread.sleep(50);
			// BEGIN: manually injected assertion regargin non-negativity invariant
			if (oldBal < 0)
				return 1;
			// END

			if (oldBal > value) {
				preparedStatement = connect.prepareStatement("select * from A where id=?");
				preparedStatement.setInt(1, key);
				rs = preparedStatement.executeQuery();
				oldBal = -10000;
				if (rs.next())
					oldBal = rs.getInt("balance");
				preparedStatement = connect.prepareStatement("update A set balance= ? where id=?");
				preparedStatement.setInt(1, oldBal - value);
				preparedStatement.setInt(2, key);
				preparedStatement.executeUpdate();
			}
			return 0;
		} catch (Exception e) {
			return -1;
		}
	}

	public static int incTransaction(Connection connect, int key, int value) throws Exception {
		try {
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");

			PreparedStatement preparedStatement = connect.prepareStatement("select * from A where id=?");
			preparedStatement.setInt(1, key);
			ResultSet rs = preparedStatement.executeQuery();
			int oldBal = -10000;
			if (rs.next())
				oldBal = rs.getInt("balance");
			preparedStatement = connect.prepareStatement("update A set balance= ? where id=?");
			preparedStatement.setInt(1, oldBal + value);
			preparedStatement.setInt(2, key);
			preparedStatement.executeUpdate();

			return 0;
		} catch (Exception e) {
			return -1;
		}
	}

	public static void initTransaction(Connection connect, int key) throws Exception {
		try {
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");

			PreparedStatement preparedStatement = connect.prepareStatement("update A set balance= 1000 where id=?");
			preparedStatement.setInt(1, key);
			preparedStatement.executeUpdate();

		} catch (Exception e) {
			throw e;
		}
	}

	/*
	 * public static int readTransaction(Connection connect, int key) throws
	 * Exception { int result = -1; try {
	 * Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");
	 * PreparedStatement preparedStatement =
	 * connect.prepareStatement("select * from A where id=?");
	 * preparedStatement.setInt(1, key); ResultSet rs =
	 * preparedStatement.executeQuery(); if (rs.next()) { result =
	 * rs.getInt("balance"); } } catch (Exception e) { throw e; } return result; }
	 */
}
