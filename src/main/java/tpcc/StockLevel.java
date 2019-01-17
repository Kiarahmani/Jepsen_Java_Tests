package tpcc;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class StockLevel {
	public static int stockLevel(CassandraConnection conn) throws Exception {
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
