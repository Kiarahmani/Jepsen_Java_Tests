package tpcc;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class Utils_tpcc {

	static AtomicBoolean atomicInitialized = new AtomicBoolean(false);
	static boolean waitForInit = true;
	static int scale = -10;

	// this function will be -dynamically- called from clojure at runtime
	public Utils_tpcc(int scale) {
		Utils_tpcc.scale = scale;
		Utils_tpcc.initialize(scale);
	}

	/*
	 * 
	 * INITIALIZATION CODE
	 * 
	 */
	public static void initialize(int scale) {
		if (atomicInitialized.compareAndSet(false, true)) {
			System.out.println("Utils_tpcc_" + scale + ": intializing data structures....");

			/*
			 * 
			 * THIS IS WHERE YOUR INITIALIZATION CODE GOES
			 * 
			 */

			waitForInit = false;
		} else {
			while (waitForInit)
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		}
	}

	/*
	 * 
	 * AUXILIARY CODE
	 * 
	 */

	public static int get_w_id() {
		return ThreadLocalRandom.current().nextInt(1, scale+1);
	}
	
	public static int get_d_id() {
		return ThreadLocalRandom.current().nextInt(1, 11);
	}
	
	public static int get_c_id() {
		return ThreadLocalRandom.current().nextInt(1, 3001);
	}
}
