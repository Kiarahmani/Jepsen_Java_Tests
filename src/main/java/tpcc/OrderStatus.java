package tpcc;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class OrderStatus {
	public static int orderStatus(CassandraConnection conn, int w_id, int d_id, boolean customerByName, int c_id,
			String c_last) throws Exception {
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
