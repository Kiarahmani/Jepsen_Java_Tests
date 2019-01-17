package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class NewOrder {
	public static int newOrder(CassandraConnection conn, int w_id, int d_id, int c_id, int o_ol_cnt, int[] itemIDs,
			int[] supplierWarehouseIDs, int[] orderQuantities) throws Exception {
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
			float w_tax = w_rs.getFloat("W_TAX");
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
			}
			int d_next_o_id = d_rs.getInt("D_NEXT_O_ID");
			float d_tax = d_rs.getFloat("D_TAX");

			stmt = conn.prepareStatement(
					"UPDATE " + "DISTRICT" + "   SET D_NEXT_O_ID = ? " + " WHERE D_W_ID = ? " + "   AND D_ID = ?");
			stmt.setInt(1, d_next_o_id + 1);
			stmt.setInt(2, w_id);
			stmt.setInt(3, d_id);
			stmt.executeUpdate();
			int o_id = d_next_o_id;

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
