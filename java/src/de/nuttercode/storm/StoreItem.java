package de.nuttercode.storm;

import de.nuttercode.storm.core.StoreCacheEntry;

/**
 * 
 * represents some item in a {@link de.nuttercode.storm.Store}
 * 
 * @author Johannes B. Latzel
 *
 * @param <T>
 *            type of the content
 */
public final class StoreItem<T> {

	/**
	 * the content of the item
	 */
	private final T content;

	/**
	 * the item of the item in the {@link de.nuttercode.storm.Store}
	 */
	private final long storeID;

	public StoreItem(StoreCacheEntry<T> storeItem) {
		assert (storeItem != null);
		storeID = storeItem.getDescription().getStoreID();
		content = storeItem.getContent();
	}

	/**
	 * @return {@link #storeID}
	 */
	public long getID() {
		return storeID;
	}

	/**
	 * @return {@link #content}
	 */
	public T getContent() {
		return content;
	}

	@Override
	public String toString() {
		return "StoreItem [storeID=" + getID() + ", content=" + getContent() + "]";
	}

}
