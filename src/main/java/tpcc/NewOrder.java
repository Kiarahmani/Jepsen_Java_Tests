package tpcc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class NewOrder {
	public static int newOrder(CassandraConnection conn, int w_id, int d_id, int c_id, int o_all_local, int o_ol_cnt,
			int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities) throws Exception {
		PreparedStatement stmt = null, stmtUpdateStock = null;

		try {
			// datastructures required for bookkeeping
			double[] itemPrices = new double[o_ol_cnt];
			String[] itemNames = new String[o_ol_cnt];
			double[] stockQuantities = new double[o_ol_cnt];
			double[] orderLineAmounts = new double[o_ol_cnt];
			double total_amount = 0;
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
			stmt.setInt(6, o_ol_cnt);
			stmt.setInt(7, o_all_local);
			stmt.executeUpdate();
			//
			stmt = conn.prepareStatement(
					"INSERT INTO " + "NEW_ORDER" + " (NO_O_ID, NO_D_ID, NO_W_ID) " + " VALUES ( ?, ?, ?)");
			stmt.setInt(1, o_id);
			stmt.setInt(2, d_id);
			stmt.setInt(3, w_id);
			stmt.executeUpdate();
			// For each O_OL_CNT item on the order perform the following tasks
			for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
				int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
				int ol_i_id = itemIDs[ol_number - 1];
				int ol_quantity = orderQuantities[ol_number - 1];
				// retrieve item
				stmt = conn
						.prepareStatement("SELECT I_PRICE, I_NAME , I_DATA " + "  FROM " + "ITEM" + " WHERE I_ID = ?");
				stmt.setInt(1, ol_i_id+1); //XXXXXXXXXXXXXXX
				//XXX remove +1 (it's just for testing!)
				ResultSet i_rs = stmt.executeQuery();
				// this is expected to happen 1% of the times
				if (!i_rs.next()) {
					if (ol_number != o_ol_cnt) {
						System.out.println("ERROR_14: Invalid item id: (" + ol_i_id
								+ ") given in the middle of the order list (unexpected)");
						return 14;
					}
					System.out.println("EXPECTED_ERROR_15: Invalid item id: (" + ol_i_id + ")");
					return 15;
				}
				double i_price = i_rs.getDouble("I_PRICE");
				String i_name = i_rs.getString("I_NAME");
				String i_data = i_rs.getString("I_DATA");
				i_rs.close();
				itemPrices[ol_number - 1] = i_price;
				itemNames[ol_number - 1] = i_name;

				// retrieve stock
				stmt = conn.prepareStatement(
						"SELECT S_REMOTE_CNT, S_ORDER_CNT,S_YTD, S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, "
								+ "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" + "  FROM " + "STOCK"
								+ " WHERE S_I_ID = ? " + "   AND S_W_ID = ?");
				stmt.setInt(1, ol_i_id+1); // XXX
				stmt.setInt(2, ol_supply_w_id+1); //XXX
				ResultSet s_rs = stmt.executeQuery();
				if (!s_rs.next()) {
					System.out.println("ERROR_16: Invalid stock primary key: (" + ol_i_id + "," + ol_supply_w_id + ")");
					return 16;
				}
				double s_quantity = s_rs.getDouble("S_QUANTITY");
				int s_ytd = s_rs.getInt("S_YTD");
				int s_order_cnt = s_rs.getInt("S_ORDER_CNT");
				int s_remote_cnt = s_rs.getInt("S_REMOTE_CNT");
				String s_data = s_rs.getString("S_DATA");
				String s_dist_01 = s_rs.getString("S_DIST_01");
				String s_dist_02 = s_rs.getString("S_DIST_02");
				String s_dist_03 = s_rs.getString("S_DIST_03");
				String s_dist_04 = s_rs.getString("S_DIST_04");
				String s_dist_05 = s_rs.getString("S_DIST_05");
				String s_dist_06 = s_rs.getString("S_DIST_06");
				String s_dist_07 = s_rs.getString("S_DIST_07");
				String s_dist_08 = s_rs.getString("S_DIST_08");
				String s_dist_09 = s_rs.getString("S_DIST_09");
				String s_dist_10 = s_rs.getString("S_DIST_10");
				s_rs.close();
				//
				stockQuantities[ol_number - 1] = s_quantity;
				if (s_quantity - ol_quantity >= 10) {
					s_quantity -= ol_quantity; // new s_quantity
				} else {
					s_quantity += -ol_quantity + 91; // new s_quantity
				}
				int s_remote_cnt_increment;
				if (ol_supply_w_id == w_id) {
					s_remote_cnt_increment = 0;
				} else {
					s_remote_cnt_increment = 1;
				}
				// update stock row
				stmtUpdateStock = conn.prepareCall("UPDATE " + "STOCK" + " SET S_QUANTITY = ?," + "S_YTD = ?,"
						+ "S_ORDER_CNT = ?," + "S_REMOTE_CNT = ? " + " WHERE S_I_ID = ? " + "   AND S_W_ID = ?");
				stmtUpdateStock.setDouble(1, s_quantity);
				stmtUpdateStock.setInt(2, s_ytd + ol_quantity);
				stmtUpdateStock.setInt(3, s_order_cnt + 1);
				stmtUpdateStock.setInt(4, s_remote_cnt + s_remote_cnt_increment);
				stmtUpdateStock.setInt(5, ol_i_id);
				stmtUpdateStock.setInt(6, ol_supply_w_id);
				stmtUpdateStock.addBatch();
				//
				double ol_amount = ol_quantity * i_price;
				orderLineAmounts[ol_number - 1] = ol_amount;
				total_amount += ol_amount;
			}
			stmtUpdateStock.executeBatch();

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
