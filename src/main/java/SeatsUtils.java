import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class SeatsUtils {
	public static long getNextCustomerId() {

		return encode(null, null, null);
		// return nextLong(281474976710656L, 80501843339247631L);

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

	private static final int COMPOSITE_BITS[] = { 48, // ID
			16, // AIRPORT_ID
	};

	protected static final long[] compositeBitsPreCompute(int offset_bits[]) {
		long pows[] = new long[offset_bits.length];
		for (int i = 0; i < offset_bits.length; i++) {
			pows[i] = (long) (Math.pow(2, offset_bits[i]) - 1l);
		} // FOR
		return (pows);
	}

	private static final long COMPOSITE_POWS[] = compositeBitsPreCompute(COMPOSITE_BITS);

	public static long encode(long values[], int offset_bits[], long offset_pows[]) {
		assert (values.length == offset_bits.length);
		long id = 0;
		int offset = 0;
		for (int i = 0; i < values.length; i++) {
			id = (i == 0 ? values[i] : id | values[i] << offset);
			offset += offset_bits[i];
		} // FOR
		return (id);
	}

}
