package tpcc;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class NewOrder {
	public static int newOrder(CassandraConnection conn, int w_id, int d_id, int c_id, int o_ol_cnt, int[] itemIDs,
			int[] supplierWarehouseIDs, int[] orderQuantities) throws Exception {
		try {
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
