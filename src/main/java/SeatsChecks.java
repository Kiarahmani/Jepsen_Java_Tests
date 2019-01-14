import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class SeatsChecks {

	public static int checkBalance(CassandraConnection conn) throws Exception {
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT balance FROM bals ALLOW FILTERING");
			ResultSet results = stmt.executeQuery();
			int all_bals = 0;
			while (results.next())
				all_bals += results.getInt("balance");

			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			// TXN SUCCESSFUL!
			// ❄❄❄❄❄❄❄❄❄❄❄❄❄❄❄
			return all_bals;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

}
