package de.nuttercode.storm.core;

import de.nuttercode.util.buffer.DynamicBuffer;

/**
 * a {@link util.buffer.DynamicBuffer} extension - can save and restore
 * {@link StoreCacheEntryDescription}
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreBuffer extends DynamicBuffer {

	/**
	 * size of a storeItemDescription in bytes
	 */
	public static final int BINARY_SIZE = Long.BYTES * 3;

	public StoreBuffer() {
		super(0, true);
	}

	/**
	 * puts the storeItemDescription in this buffer
	 * 
	 * @param storeItemDescription
	 */
	public void putStoreItemDescription(StoreCacheEntryDescription storeItemDescription) {
		putLong(storeItemDescription.getStoreID());
		putLong(storeItemDescription.getStoreLocation().getBegin());
		putLong(storeItemDescription.getStoreLocation().getEnd());
	}

	/**
	 * @param index
	 * @return {@link StoreCacheEntryDescription} given by this buffers content and
	 *         the given index or null if begin and end of the description are 0
	 */
	public StoreCacheEntryDescription getStoreItemDescription(long index) {
		long storeID = getLong();
		long begin = getLong();
		long end = getLong();
		if (begin == 0 && end == 0) {
			return null;
		}
		return new StoreCacheEntryDescription(new StoreLocation(begin, end), storeID, index);
	}

}
