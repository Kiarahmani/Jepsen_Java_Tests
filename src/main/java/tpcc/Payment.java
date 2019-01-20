package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class Payment {
	@SuppressWarnings("resource")
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
				System.out.println("ERROR_22: Invalid district id: " + w_id + "," + d_id);
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

			//
			// Retrieve customer's information

			String c_first, c_middle, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit;
			double c_credit_lim, c_discount, c_balance;
			float c_ytd_payment;
			int c_payment_cnt;
			Timestamp c_since;
			if (customerByName) {
				stmt = conn.prepareStatement("SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2,"
						+ "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM,"
						+ "   C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " + "  FROM " + "CUSTOMER"
						+ " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + "   AND C_LAST = ? " + "ALLOW FILTERING");
				stmt.setInt(1, customerWarehouseID);
				stmt.setInt(2, customerDistrictID);
				stmt.setString(3, c_last);
				ResultSet c_rs = stmt.executeQuery();
				// find the appropriate index ano
				int index = 0;
				List<Integer> all_c_ids = new ArrayList<Integer>();
				while (c_rs.next()) {
					index++;
					all_c_ids.add(c_rs.getInt("C_ID"));
				}
				System.out.println("all c_ids:   " + all_c_ids);
				if (index == 0) {
					System.out.println("ERROR_23: No customer with the given last name: " + customerWarehouseID + ","
							+ customerDistrictID + "," + c_last);
					return 23;
				}
				if (index % 2 != 0)
					index++;
				index = (index / 2);
				int chose_c_id = all_c_ids.get(index-1);
				System.out.println("chosen c_id: " + chose_c_id);
				c_rs.absolute(index);
				System.out.println("------------>"+c_rs.getInt("C_ID"));
				
				
				
			} else {
				// retrieve customer by id
				stmt = conn.prepareStatement("SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, "
						+ "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, "
						+ "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " + "  FROM "
						+ "CUSTOMER" + " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + "   AND C_ID = ?");
				stmt.setInt(1, customerWarehouseID);
				stmt.setInt(2, customerDistrictID);
				stmt.setInt(3, c_id);
				ResultSet c_rs = stmt.executeQuery();
				if (!c_rs.next()) {
					System.out.println("ERROR_24: Invalid customer id: " + customerWarehouseID + ","
							+ customerDistrictID + "," + c_id);
					return 24;
				}
				c_first = c_rs.getString("c_first");
				c_middle = c_rs.getString("c_middle");
				c_street_1 = c_rs.getString("c_street_1");
				c_street_2 = c_rs.getString("c_street_2");
				c_city = c_rs.getString("c_city");
				c_state = c_rs.getString("c_state");
				c_zip = c_rs.getString("c_zip");
				c_phone = c_rs.getString("c_phone");
				c_credit = c_rs.getString("c_credit");
				c_credit_lim = c_rs.getDouble("c_credit_lim");
				c_discount = c_rs.getDouble("c_discount");
				c_balance = c_rs.getDouble("c_balance");
				c_ytd_payment = c_rs.getFloat("c_ytd_payment");
				c_payment_cnt = c_rs.getInt("c_payment_cnt");
				c_since = c_rs.getTimestamp("c_since");
				//
				// update columns

			}

			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			stmt.clearBatch();
			stmt.close();
			return 0;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
}
