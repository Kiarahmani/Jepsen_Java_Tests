package tpcc;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class OrderStatus {
	public static int orderStatus(CassandraConnection conn) throws Exception {
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
