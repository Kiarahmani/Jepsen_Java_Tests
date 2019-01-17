package tpcc;

import java.util.concurrent.atomic.AtomicBoolean;

public class Utils_tpcc {

	static AtomicBoolean atomicInitialized = new AtomicBoolean(false);
	static boolean waitForInit = true;

	// this function will be -dynamically- called from clojure at runtime
	public Utils_tpcc(int scale) {
		Utils_tpcc.initialize(scale);
	}

	/*
	 * 
	 * 
	 */
	public static void initialize(int scale) {
		if (atomicInitialized.compareAndSet(false, true)) {
			System.out.println("Utils_tpcc_"+scale+": intializing data structures....");
			
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
}
