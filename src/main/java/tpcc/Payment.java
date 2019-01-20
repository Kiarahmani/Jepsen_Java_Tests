package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class Payment {
	public static int payment(CassandraConnection conn, int w_id, int d_id, boolean customerByName, int c_id,
			String c_last, int customerDistrictID, double paymentAmount) throws Exception {
		PreparedStatement stmt = null;
		try {
			boolean isRemote = (w_id != customerDistrictID);
			double w_ydt;
			String w_street_1, w_street_2, w_city, w_state, w_zip, w_name;
			String d_street_1, d_street_2, d_city, d_state, d_zip, d_name;
			// read necessary columns from warehouse
			stmt = conn.prepareStatement("SELECT W_YTD, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME"
					+ "  FROM " + "WAREHOUSE" + " WHERE W_ID = ?");
			stmt.setInt(1, w_id);
			ResultSet w_rs = stmt.executeQuery();
			if (!w_rs.next()) {
				System.out.println("ERROR_11: Invalid warehouse id: " + w_id);
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
