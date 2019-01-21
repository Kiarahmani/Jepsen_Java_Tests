package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class Delivery {
	private static boolean _VERBOSE = false;

	public static int delivery(CassandraConnection conn, int w_id, int o_carrier_id) throws Exception {
		try {

			PreparedStatement stmt = null;
			int d_id;
			int[] orderIDs = new int[10];

			for (d_id = 1; d_id <= 10; d_id++) {
				stmt = conn.prepareStatement("SELECT NO_O_ID FROM " + "NEW_ORDER" + " WHERE NO_D_ID = ? "
						+ "   AND NO_W_ID = ? " + " ORDER BY no_d_id,no_o_id" + " LIMIT 1 ALLOW FILTERING");
				stmt.setInt(1, d_id);
				stmt.setInt(2, w_id);
				ResultSet no_rs = stmt.executeQuery();
				if (!no_rs.next()) {
					// This district has no new orders
					// This can happen but should be rare
					if (_VERBOSE)
						System.out.println(String.format("District has no new orders [W_ID=%d, D_ID=%d]", w_id, d_id));
					continue;
				}
				int no_o_id = no_rs.getInt("NO_O_ID");
				orderIDs[d_id - 1] = no_o_id;
				no_rs.close();
				// delete the row containing the oldest order
				stmt = conn.prepareStatement("DELETE FROM " + "NEW_ORDER" + " WHERE NO_O_ID = ? " + " AND NO_D_ID = ?"
						+ "   AND NO_W_ID = ?");

				stmt.setInt(1, no_o_id);
				stmt.setInt(2, d_id);
				stmt.setInt(3, w_id);
				stmt.executeUpdate();

				// retrieve order
				stmt = conn.prepareStatement("SELECT O_C_ID FROM " + "OORDER" + " WHERE O_ID = ? "
						+ "   AND O_D_ID = ? " + "   AND O_W_ID = ?");
				stmt.setInt(1, no_o_id);
				stmt.setInt(2, d_id);
				stmt.setInt(3, w_id);
				ResultSet oo_rs = stmt.executeQuery();
				if (!oo_rs.next()) {
					System.out.println(
							String.format("ERROR_41: Failed to retrieve ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]", w_id,
									d_id, no_o_id));
					return 41;
				}
				int c_id = oo_rs.getInt("O_C_ID");
				oo_rs.close();
				System.out.println("###" + c_id);

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
