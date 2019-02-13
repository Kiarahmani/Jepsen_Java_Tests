package bank;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.github.adejanovski.cassandra.jdbc.CassandraConnection;

public class Withdraw {
	public static int withdraw(CassandraConnection conn, int id, int amount) throws Exception {
		try {
			PreparedStatement stmt = conn.prepareStatement("SELECT balance FROM accounts WHERE id = ? ");
			stmt.setInt(1, id);
			ResultSet results = stmt.executeQuery();
			int old_bal = results.getInt("balance");
			//
			if (old_bal < 0)
				return 2;
			if (old_bal >= amount) {
				stmt = conn.prepareStatement("UPDATE accounts SET balance = ? WHERE id = ? ");
				stmt.setInt(1, old_bal - amount);
				stmt.setInt(2, id);
				stmt.executeUpdate();
			}

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
