package bank;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils_bank {

	static AtomicBoolean atomicInitialized = new AtomicBoolean(false);
	static boolean waitForInit = true;
	static int scale = -10;
	private static Random r = new Random();
	private static AtomicInteger count = new AtomicInteger(-1);

	// this function will be -dynamically- called from clojure at runtime
	public Utils_bank(int scale) {
		Utils_bank.scale = scale;
		Utils_bank.initialize(scale);
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

}
