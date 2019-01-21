package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class StockLevel {
	public static int stockLevel(CassandraConnection conn, int w_id, int d_id, int threshold) throws Exception {
		PreparedStatement stmt = null;
		try {

			//
			// retrieve the latest order_id from the given district
			stmt = conn.prepareStatement(
					"SELECT D_NEXT_O_ID " + "  FROM " + "DISTRICT" + " WHERE D_W_ID = ? " + "   AND D_ID = ?");
			stmt.setInt(1, w_id);
			stmt.setInt(2, d_id);
			ResultSet d_rs = stmt.executeQuery();
			if (!d_rs.next()) {
				System.out.println("ERROR_51: district does not exist: " + w_id + "," + d_id);
				return 51;
			}
			int o_id = d_rs.getInt("D_NEXT_O_ID");
			d_rs.close();

			//
			// retrieve the latest 20 orders
			stmt = conn.prepareStatement(
					"select ol_i_id from order_line WHERE ol_w_id=? and ol_d_id=? and OL_O_ID < ? and ol_o_id > ?");
			stmt.setInt(1, w_id);
			stmt.setInt(2, d_id);
			stmt.setInt(3, o_id);
			stmt.setInt(4, (o_id - 20));
			ResultSet ol_rs = stmt.executeQuery();
			while (ol_rs.next()) {
				int ol_i_id = ol_rs.getInt("ol_i_id");
				System.out.println(ol_i_id);
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
