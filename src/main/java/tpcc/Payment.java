package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class Payment {
	public static int payment(CassandraConnection conn, int w_id, int d_id, boolean customerByName, int c_id,
			String c_last, int customerWarehouseID, int customerDistrictID, double paymentAmount) throws Exception {
		PreparedStatement stmt = null;
		try {
			boolean isRemote = (w_id != customerDistrictID);
			double w_ydt, d_ytd;
			String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
			String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;
			// read necessary columns from warehouse
			stmt = conn.prepareStatement("SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME"
					+ "  FROM " + "WAREHOUSE" + " WHERE W_ID = ?");
			stmt.setInt(1, w_id);
			ResultSet w_rs = stmt.executeQuery();
			if (!w_rs.next()) {
				System.out.println("ERROR_21: Invalid warehouse id: " + w_id);
				return 21;
			}
			w_ydt = w_rs.getDouble("W_YTD");
			w_street_1 = w_rs.getString("W_STREET_1");
			w_street_2 = w_rs.getString("W_STREET_2");
			w_city = w_rs.getString("W_CITY");
			w_state = w_rs.getString("W_STATE");
			w_zip = w_rs.getString("W_ZIP");
			w_name = w_rs.getString("W_NAME");
			w_rs.close();
			//
			// update W_YTD by paymentAmount
			stmt = conn.prepareStatement("UPDATE " + "WAREHOUSE" + "   SET W_YTD = ? " + " WHERE W_ID = ? ");
			stmt.setDouble(1, w_ydt + paymentAmount);
			stmt.setInt(2, w_id);
			stmt.executeUpdate();

			//
			// read necessary columns from district
			stmt = conn.prepareStatement("SELECT D_YTD, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME"
					+ "  FROM " + "DISTRICT" + " WHERE D_W_ID = ? " + "   AND D_ID = ?");
			stmt.setInt(1, w_id);
			stmt.setInt(2, d_id);
			ResultSet d_rs = stmt.executeQuery();
			if (!d_rs.next()) {
				System.out.println("ERROR_22: Invalid district id: " + d_id);
				return 22;
			}
			d_ytd = d_rs.getDouble("D_YTD");
			d_street_1 = d_rs.getString("D_STREET_1");
			d_street_2 = d_rs.getString("D_STREET_2");
			d_city = d_rs.getString("D_CITY");
			d_state = d_rs.getString("D_STATE");
			d_zip = d_rs.getString("D_ZIP");
			d_name = d_rs.getString("D_NAME");
			d_rs.close();
			//
			// update D_YTD by paymentAmount
			stmt = conn
					.prepareStatement("UPDATE " + "DISTRICT" + "   SET D_YTD = ? " + " WHERE D_W_ID = ? AND D_ID = ? ");
			stmt.setDouble(1, d_ytd + paymentAmount);
			stmt.setInt(2, w_id);
			stmt.setInt(3, d_id);
			stmt.executeUpdate();

			if (customerByName) {

			} else {
				// retrieve customer by id
				stmt = conn.prepareCall("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
						+ "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
						+ "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " + "  FROM "
						+ "CUSTOMER" + " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + "   AND C_ID = ?");
				stmt.setInt(1, customerWarehouseID);
				stmt.setInt(2, customerDistrictID);
				stmt.setInt(3, c_id);
			}

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
