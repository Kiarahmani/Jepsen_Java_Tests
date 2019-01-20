package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class OrderStatus {
	public static int orderStatus(CassandraConnection conn, int w_id, int d_id, boolean customerByName, int c_id,
			String c_last) throws Exception {
		try {
			PreparedStatement stmt = null;
			ResultSet c_rs = null;
			if (customerByName) {
				stmt = conn.prepareStatement("SELECT C_ID" + "  FROM " + "CUSTOMER" + " WHERE C_W_ID = ? "
						+ "   AND C_D_ID = ? " + "   AND C_LAST = ? " + "ALLOW FILTERING");
				stmt.setInt(1, w_id);
				stmt.setInt(2, d_id);
				stmt.setString(3, c_last);
				c_rs = stmt.executeQuery();
				// find the appropriate index
				int index = 0;
				List<Integer> all_c_ids = new ArrayList<Integer>();
				while (c_rs.next()) {
					index++;
					all_c_ids.add(c_rs.getInt("C_ID"));
				}
				if (index == 0) {
					System.out.println(
							"ERROR_23: No customer with the given last name: " + w_id + "," + d_id + "," + c_last);
					return 23;
				}
				if (index % 2 != 0)
					index++;
				index = (index / 2);
				c_id = all_c_ids.get(index - 1);
				c_rs.close();
			}
			// now retrive the customer's data based on the given c_id (or the chosen one in
			// case of customerByName)
			stmt = conn.prepareStatement("SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE" + " FROM CUSTOMER"
					+ " WHERE C_W_ID = ? " + " AND C_D_ID = ? " + " AND C_ID = ?");
			stmt.setInt(1, w_id);
			stmt.setInt(2, d_id);
			stmt.setInt(3, c_id);
			c_rs = stmt.executeQuery();
			String c_first = c_rs.getString("C_FIRST");
			String c_middle = c_rs.getString("C_MIDDLE");
			c_last = c_rs.getString("C_LAST");
			double c_balance = c_rs.getDouble("C_BALANCE");
			c_rs.close();
			//
			// retrieve orders by this customer
			stmt = conn.prepareStatement(
					"SELECT O_ID, O_CARRIER_ID, O_ENTRY_D " + "  FROM " + "OORDER" + " WHERE O_W_ID = ? "
							+ "   AND O_D_ID = ? " + "   AND O_C_ID = ? " + " order by O_ID ALLOW FILTERING");
			stmt.setInt(1, w_id);
			stmt.setInt(2, d_id);
			stmt.setInt(3, c_id);
			ResultSet o_rs = stmt.executeQuery();
			System.out.println("---------");
			while(o_rs.next())
				System.out.println(o_rs.getInt("O_ID"));
			System.out.println("---------");

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
