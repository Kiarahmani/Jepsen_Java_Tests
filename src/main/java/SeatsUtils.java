import java.util.concurrent.ThreadLocalRandom;

public class SeatsUtils {
	public static long getNextCustomerId() {
		return nextLong(281474976710656L, 80501843339247631L);
	}

	public static long getNextAirlineId() {
		return nextLong(281474976710656L, 80501843339247631L);
	}
	
	public static long getNextFlightId() {
		return nextLong(89124701353L, 87750050550350462L);
	}
	
	
	
	
	public static long nextLong(long minimum, long maximum) {
		return ThreadLocalRandom.current().nextLong(minimum, maximum);
	}
}
