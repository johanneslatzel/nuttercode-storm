package de.nuttercode.storm.core;

import de.nuttercode.util.test.LongInterval;

/**
 * some location in the store defined by its relative begin and (off the) end
 * positions
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreLocation extends LongInterval {

	public StoreLocation(long begin, long end) {
		super(begin, end);
	}

}
