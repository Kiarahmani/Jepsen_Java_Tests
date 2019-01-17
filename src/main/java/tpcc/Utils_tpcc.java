package tpcc;

import java.util.concurrent.atomic.AtomicBoolean;

public class Utils_tpcc {

	static AtomicBoolean atomicInitialized = new AtomicBoolean(false);
	static boolean waitForInit = true;

	// this function will be -dynamically- called from clojure at runtime
	public Utils_tpcc() {
		Utils_tpcc.initialize();
	}

	/*
	 * 
	 * 
	 */
	public static void initialize() {
		if (atomicInitialized.compareAndSet(false, true)) {
			System.out.println("Utils_tpcc: intializing data structures....");
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
}
