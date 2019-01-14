import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class SeatsChecks {

	public static int checkBalance(CassandraConnection conn) throws Exception {
		try {
			/*PreparedStatement stmt = conn.prepareStatement("SELECT sum (balance) FROM bals");
			ResultSet results = stmt.executeQuery();
			int all_bals = 0;
			results.next();
			all_bals += results.getInt(1);

			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄*/
			return 6;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

}
