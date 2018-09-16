package de.nuttercode.storm.core;

/**
 * describes a StoreCacheEntry
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreCacheEntryDescription {

	/**
	 * location of the StoreItem in the Store
	 */
	private final StoreLocation storeLocation;

	/**
	 * id of the StoreItem
	 */
	private final long storeID;

	/**
	 * index in the StoreItemTable
	 */
	private final long index;

	public StoreCacheEntryDescription(StoreLocation storeLocation, long storeID, long index) {
		assert (storeLocation != null);
		assert (index >= 0);
		this.storeID = storeID;
		this.storeLocation = storeLocation;
		this.index = index;
	}

	public long getStoreID() {
		return storeID;
	}

	public StoreLocation getStoreLocation() {
		return storeLocation;
	}

	public long getIndex() {
		return index;
	}

	@Override
	public String toString() {
		return "StoreItemDescription [storeLocation=" + storeLocation + ", storeID=" + storeID + ", index=" + index
				+ "]";
	}

}
