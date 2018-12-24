import java.util.concurrent.ThreadLocalRandom;

public class SeatsUtils {
	public static long getNextCustomerId() {
		return nextLong(281474976710656L, 80501843339247631L);
	}

	public static long nextLong(long minimum, long maximum) {
		return ThreadLocalRandom.current().nextLong(minimum, maximum);
	}
}
