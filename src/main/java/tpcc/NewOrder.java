package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class NewOrder {
	public static int newOrder(CassandraConnection conn, int w_id, int d_id, int c_id, int o_all_local, int o_ol_cnt,
			int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities) throws Exception {
		PreparedStatement stmt = null;

		try {
			// retrieve w_tax rate
			stmt = conn.prepareStatement("SELECT W_TAX " + "  FROM " + "WAREHOUSE" + " WHERE W_ID = ?");
			stmt.setInt(1, w_id);
			ResultSet w_rs = stmt.executeQuery();
			if (!w_rs.next()) {
				System.out.println("ERROR_11: Invalid warehouse id: " + w_id);
				return 11;
			}
			double w_tax = w_rs.getDouble("W_TAX");
			w_rs.close();
			//
			//
			// Q? XXX should I close the stmt before reassigning it? does it affect the
			// performance?
			// retrieve d_tax rate and update D_NEXT_O_ID
			stmt = conn.prepareStatement(
					"SELECT D_NEXT_O_ID, D_TAX " + "  FROM " + "DISTRICT" + " WHERE D_W_ID = ? AND D_ID = ?");
			stmt.setInt(1, w_id);
			stmt.setInt(2, d_id);
			ResultSet d_rs = stmt.executeQuery();
			if (!d_rs.next()) {
				System.out.println("ERROR_12: Invalid district id: (" + w_id + "," + d_id + ")");
				return 12;
			}
			int d_next_o_id = d_rs.getInt("D_NEXT_O_ID");
			double d_tax = d_rs.getDouble("D_TAX");

			stmt = conn.prepareStatement(
					"UPDATE " + "DISTRICT" + "   SET D_NEXT_O_ID = ? " + " WHERE D_W_ID = ? " + "   AND D_ID = ?");
			stmt.setInt(1, d_next_o_id + 1);
			stmt.setInt(2, w_id);
			stmt.setInt(3, d_id);
			stmt.executeUpdate();
			int o_id = d_next_o_id;

			//
			// retrieve customer's information
			stmt = conn.prepareStatement("SELECT C_DISCOUNT, C_LAST, C_CREDIT" + "  FROM " + "CUSTOMER"
					+ " WHERE C_W_ID = ? " + "   AND C_D_ID = ? " + "   AND C_ID = ?");
			stmt.setInt(1, w_id);
			stmt.setInt(2, d_id);
			stmt.setInt(3, c_id);
			ResultSet c_rs = stmt.executeQuery();
			if (!c_rs.next()) {
				System.out.println("ERROR_13: Invalid customer id: (" + w_id + "," + d_id + "," + c_id + ")");
				return 13;
			}
			double c_discount = c_rs.getDouble("C_DISCOUNT");
			String c_last = c_rs.getString("C_LAST");
			String c_credit = c_rs.getString("C_CREDIT");

			//
			// insert a new row into OORDER and NEW_ORDER tables
			stmt = conn.prepareStatement(
					"INSERT INTO " + "OORDER" + " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)"
							+ " VALUES (?, ?, ?, ?, ?, ?, ?)");
			stmt.setInt(1, o_id);
			stmt.setInt(2, d_id);
			stmt.setInt(3, w_id);
			stmt.setInt(4, c_id);
			stmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
			System.out.println();
			System.out.println(o_ol_cnt);
			System.out.println(o_all_local);
			System.out.println();
			stmt.setInt(6, o_ol_cnt);
			stmt.setInt(7, o_all_local);
			stmt.executeUpdate();

			//
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
//
//
//
//
//
//
