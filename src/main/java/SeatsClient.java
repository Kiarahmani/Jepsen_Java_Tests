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

	public static int writeTransaction(Connection connect, int key, int value) throws Exception {
		try {
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");

			PreparedStatement preparedStatement1 = connect.prepareStatement("update a set balance= ? where id=?");
			preparedStatement1.setInt(1, value);
			preparedStatement1.setInt(2, key);
			preparedStatement1.executeUpdate();

			PreparedStatement preparedStatement2 = connect.prepareStatement("update b set balance= ? where id=?");
			preparedStatement2.setInt(1, value);
			preparedStatement2.setInt(2, key);
			preparedStatement2.executeUpdate();

			return 0;
		} catch (Exception e) {
			return -1;
		}
	}

	public static int tempTransaction(Connection connect, int key, int value) throws Exception {
		try {
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");

			PreparedStatement preparedStatement1 = connect.prepareStatement("select * from a where id=?");
			preparedStatement1.setInt(1, key);
			ResultSet rs1 = preparedStatement1.executeQuery();
			int bala = -10000;
			if (rs1.next())
				bala = rs1.getInt("balance");

			return 0;
		} catch (Exception e) {
			return -1;
		}
	}

	public static int readTransaction(Connection connect, int key, int value) throws Exception {
		try {
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");

			PreparedStatement preparedStatement1 = connect.prepareStatement("select * from a where id=?");
			preparedStatement1.setInt(1, key);
			ResultSet rs1 = preparedStatement1.executeQuery();
			int bala = -10000;
			if (rs1.next())
				bala = rs1.getInt("balance");

			PreparedStatement preparedStatement2 = connect.prepareStatement("select * from b where id=?");
			preparedStatement2.setInt(1, key);
			ResultSet rs2 = preparedStatement2.executeQuery();
			int balb = -10000;
			if (rs2.next())
				balb = rs2.getInt("balance");
			if (bala == balb)
				return 0;
			else
				return 1;
		} catch (Exception e) {
			return -1;
		}
	}

}
