package tpcc;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class Payment {
	public static int payment(CassandraConnection conn, int w_id, int d_id, boolean customerByName, int c_id,
			String c_last, int customerDistrictID, double paymentAmount) throws Exception {
		try {
			boolean isRemote = (w_id != customerDistrictID);

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
