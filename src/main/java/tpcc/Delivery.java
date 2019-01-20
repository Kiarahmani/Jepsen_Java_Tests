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
			for (d_id = 1; d_id <= 10; d_id++) {
				stmt = conn.prepareStatement("SELECT NO_O_ID FROM " + "NEW_ORDER" + " WHERE NO_D_ID = ? "
						+ "   AND NO_W_ID = ? " + " ORDER BY NO_O_ID ASC " + " LIMIT 1");
				stmt.setInt(1, d_id);
				stmt.setInt(2, w_id);
				ResultSet rs = stmt.executeQuery();
				if (!rs.next()) {
					// This district has no new orders
					// This can happen but should be rare
					if (_VERBOSE)
						System.out.println(String.format("District has no new orders [W_ID=%d, D_ID=%d]", w_id, d_id));
					continue;
				}
				int no_o_id = rs.getInt("NO_O_ID");
				System.out.println("###" + no_o_id);
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
