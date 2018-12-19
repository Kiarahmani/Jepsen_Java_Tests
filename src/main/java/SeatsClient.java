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
			connect = DriverManager.getConnection("jdbc:cassandra://" + localAddr + ":9042/seats");
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

	/*
	 * 
	 * (1) DELETE RESERVATION
	 * 
	 */
	public static int deleteReservation(Connection conn, int f_id, int c_id, String c_id_str, String ff_c_id_str,
			int ff_al_id) throws Exception {
		try {
			Class.forName("com.github.adejanovski.cassandra.jdbc.CassandraDriver");

			PreparedStatement stmt = null;
			// If we weren't given the customer id, then look it up
			if (c_id == -1) {
				// Use the customer's id as a string
				assert (c_id_str != null && c_id_str.length() > 0);
				stmt = conn.prepareStatement("SELECT C_ID FROM CUSTOMER WHERE C_ID_STR = ?");
				stmt.setString(1, c_id_str);
				ResultSet results = stmt.executeQuery();
				if (results.next()) {
					c_id = results.getInt("C_ID");
				} else {
					results.close();
					throw new Exception(
							String.format("No Customer record was found [c_id_str=%s, ff_c_id_str=%s, ff_al_id=%s]",
									c_id_str, ff_c_id_str, ff_al_id));
				}
				results.close();
			}

			// Now get the result of the information that we need
			// If there is no valid customer record, then throw an abort
			// This should happen 5% of the time
			// XXX We will in fact chop the original query with join on three table into
			// three
			// separate queries. We also read extra columns which will be used later when
			// updating them
			// 1
			stmt = conn.prepareStatement(
					"SELECT C_SATTR00, C_SATTR02, C_SATTR04, C_IATTR00, C_IATTR02, C_IATTR04, C_IATTR06, C_BALANCE, C_IATTR10, C_IATTR11 FROM CUSTOMER WHERE C_ID = ?");
			stmt.setInt(1, c_id);
			ResultSet results2 = stmt.executeQuery();
			if (results2.next() == false) {
				results2.close();
				return -2;
				// throw new Exception(String.format("No Customer information record found for
				// id '%d'", c_id));

			}
			/*
			int oldBal = results2.getInt("C_BALANCE");
			int oldAttr10 = results2.getInt("C_IATTR10");
			int oldAttr11 = results2.getInt("C_IATTR11");
			int c_iattr00 = results2.getInt("C_SATTR00") + 1;
			// 2
			stmt = conn.prepareStatement("SELECT F_SEATS_LEFT FROM FLIGHT WHERE F_ID = ? ");
			stmt.setInt(1, f_id);
			ResultSet results3 = stmt.executeQuery();
			results3.next();
			int seats_left = results3.getInt(0);
			// 3
			stmt = conn.prepareStatement(
					"SELECT R_ID, R_SEAT, R_PRICE, R_IATTR00 FROM RESERVATION WHERE R_C_ID = ? AND R_F_ID = ? ");
			stmt.setInt(1, c_id);
			stmt.setInt(2, f_id);
			ResultSet results4 = stmt.executeQuery();
			int r_id = results4.getInt("R_ID");
			double r_price = results4.getDouble("R_PRICE");
			results4.close();
			int updated = 0;

			// Now delete all of the flights that they have on this flight
			stmt = conn.prepareStatement("DELETE FROM RESERVATION WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ?");
			stmt.setInt(1, r_id);
			stmt.setInt(2, c_id);
			stmt.setInt(3, f_id);
			updated = stmt.executeUpdate();
			assert (updated == 1);

			// Update Available Seats on Flight
			stmt = conn.prepareStatement("UPDATE FLIGHT SET F_SEATS_LEFT = ?" + " WHERE F_ID = ? ");
			stmt.setInt(1, seats_left + 1);
			stmt.setInt(2, f_id);
			updated = stmt.executeUpdate();
			assert (updated == 1);

			// Update Customer's Balance
			stmt = conn.prepareStatement(
					"UPDATE CUSTOMER SET C_BALANCE = ?, C_IATTR00 = ?, C_IATTR10 = ?,  C_IATTR11 = ? WHERE C_ID = ? ");
			stmt.setInt(1, oldBal + (int) (-1 * r_price));
			stmt.setInt(2, c_iattr00);
			stmt.setInt(3, oldAttr10 - 1);
			stmt.setInt(4, oldAttr11 - 1);
			stmt.setInt(5, c_id);
			updated = stmt.executeUpdate();
			assert (updated == 1);

			// Update Customer's Frequent Flyer Information (Optional)
			if (ff_al_id != -1) {
				stmt = conn.prepareStatement(
						"SELECT FF_IATTR10 FROM FREQUENT_FLYER " + " WHERE FF_C_ID = ? " + "   AND FF_AL_ID = ?");
				stmt.setInt(1, c_id);
				stmt.setInt(2, ff_al_id);
				ResultSet results5 = stmt.executeQuery();
				results5.next();
				int olAttr10 = results5.getInt(0);
				stmt = conn.prepareStatement(
						"UPDATE FREQUENT_FLYER SET FF_IATTR10 = ?" + " WHERE FF_C_ID = ? " + "   AND FF_AL_ID = ?");
				stmt.setInt(1, olAttr10 - 1);
				stmt.setInt(2, c_id);
				stmt.setInt(3, ff_al_id);
				updated = stmt.executeUpdate();
				assert (updated == 1) : String.format("Failed to update FrequentFlyer info [c_id=%d, ff_al_id=%d]",
						c_id, ff_al_id);
			}
*/
			return 0;
		} catch (Exception e) {
			return -1;
		}
	}

}
