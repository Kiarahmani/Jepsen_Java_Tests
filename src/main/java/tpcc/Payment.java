package tpcc;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class Payment {
	public static int payment(CassandraConnection conn, int w_id, int d_id, boolean customerByName, int c_id,
			String c_last, int customerDistrictID, int paymentAmount) throws Exception {
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
