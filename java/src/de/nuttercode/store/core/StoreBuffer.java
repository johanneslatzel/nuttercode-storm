package de.nuttercode.store.core;

import de.nuttercode.util.buffer.DynamicBuffer;

/**
 * a {@link util.buffer.DynamicBuffer} extension - can save and restore
 * {@link StoreCacheEntryDescription}
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreBuffer extends DynamicBuffer {

	public static final int BINARY_SIZE = Long.BYTES * 3;

	public StoreBuffer() {
		super(0, true);
	}

	public void putStoreItemDescription(StoreCacheEntryDescription storeItemDescription) {
		putLong(storeItemDescription.getStoreID());
		putLong(storeItemDescription.getStoreLocation().getBegin());
		putLong(storeItemDescription.getStoreLocation().getEnd());
	}

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
