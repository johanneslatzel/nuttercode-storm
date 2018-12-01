package de.nuttercode.storm.core;

import de.nuttercode.storm.Store;
import de.nuttercode.storm.StoreItem;
import de.nuttercode.util.LongInterval;

/**
 * describes a {@link StoreItem}
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreItemDescription {

	/**
	 * location of the StoreItem in the Store
	 */
	private final LongInterval storeLocation;

	/**
	 * id of the StoreItem
	 */
	private final long storeID;

	/**
	 * index in the StoreItemTable
	 */
	private final long index;

	/**
	 * called by {@link Store} and its components. don't call this constructor.
	 * manually.
	 * 
	 * @param storeLocation
	 * @param storeID
	 * @param index
	 */
	public StoreItemDescription(LongInterval storeLocation, long storeID, long index) {
		assert (storeLocation != null);
		assert (index >= 0);
		this.storeID = storeID;
		this.storeLocation = storeLocation;
		this.index = index;
	}

	/**
	 * @return id of the StoreItem
	 */
	public long getStoreID() {
		return storeID;
	}

	/**
	 * @return location of the {@link StoreItem} in the {@link Store}
	 */
	public LongInterval getStoreLocation() {
		return storeLocation;
	}

	/**
	 * @return index in the {@link StoreItem} in the StoreItemTable
	 */
	public long getIndex() {
		return index;
	}

	@Override
	public String toString() {
		return "StoreItemDescription [storeLocation=" + storeLocation + ", storeID=" + storeID + ", index=" + index
				+ "]";
	}

}
