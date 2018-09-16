package de.nuttercode.storm.core;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import de.nuttercode.storm.Store;
import de.nuttercode.storm.StoreItem;
import de.nuttercode.util.Assurance;

/**
 * manages all {@link StoreItem}s in a {@link Store}
 * 
 * @author Johannes B. Latzel
 *
 * @param <T>
 *            some content type
 */
public final class StoreItemManager<T> {

	/**
	 * maps {@link StoreItem} ids to {@link StoreCacheEntry}s
	 */
	private final Map<Long, StoreCacheEntry<T>> itemMap;

	public StoreItemManager() {
		itemMap = new HashMap<>();
	}

	/**
	 * @param storeID
	 * @throws NoSuchElementException
	 *             if there is no {@link StoreCacheEntry} with the specified storeID
	 */
	private void assureContains(long storeID) {
		if (!contains(storeID))
			throw new NoSuchElementException("there is no item in the Store with the id " + storeID);
	}

	/**
	 * @param storeID
	 * @return true if a {@link StoreCacheEntry} with the specified id exists
	 */
	public boolean contains(long storeID) {
		return itemMap.containsKey(storeID);
	}

	/**
	 * creates a new {@link StoreCacheEntry} for the specified storeItemDescription
	 * 
	 * @param storeItemDescription
	 * @throws IllegalArgumentException
	 *             if storeItemDescription is null
	 */
	public void newItem(StoreCacheEntryDescription storeItemDescription) {
		Assurance.assureNotNull(storeItemDescription);
		itemMap.put(storeItemDescription.getStoreID(), new StoreCacheEntry<>(storeItemDescription));
	}

	/**
	 * clears all cached content of every {@link StoreCacheEntry}
	 */
	public void clearCache() {
		for (StoreCacheEntry<T> item : itemMap.values())
			item.setContent(null);
	}

	/**
	 * @return a {@link SortedSet} of ids of all {@link StoreItem}s
	 */
	public SortedSet<Long> getStoreIDSet() {
		return new TreeSet<>(itemMap.keySet());
	}

	/**
	 * @param storeID
	 * @return a {@link StoreItem} specified by the storeID
	 * @throws NoSuchElementException
	 *             if there is no {@link StoreCacheEntry} with the specified storeID
	 */
	public StoreItem<T> get(long storeID) {
		assureContains(storeID);
		return itemMap.get(storeID).createStoreItem();
	}

	/**
	 * sets the entry in the cache to the corresponding storeID to the item
	 * 
	 * @param storeID
	 * @param item
	 */
	public void set(long storeID, StoreCacheEntry<T> item) {
		itemMap.put(storeID, item);
	}

	/**
	 * sets the content of an existing entrx
	 * 
	 * @param storeID
	 * @param content
	 * @throws NoSuchElementException
	 *             if {@link #assureContains(long)} does for storeID
	 */
	public void set(long storeID, T content) {
		assureContains(storeID);
		itemMap.get(storeID).setContent(content);
	}

	/**
	 * removes the entry corresponding to the storeID from this manager
	 * 
	 * @param storeID
	 * @throws NoSuchElementException
	 *             if {@link #assureContains(long)} does
	 */
	public void remove(long storeID) {
		assureContains(storeID);
		itemMap.remove(storeID);
	}

	/**
	 * @param storeID
	 * @return {@link StoreLocation} of the entry corresponding to the storeID
	 * @throws NoSuchElementException
	 *             if {@link #assureContains(long)} does
	 */
	public StoreLocation getStoreLocation(long storeID) {
		assureContains(storeID);
		return itemMap.get(storeID).getDescription().getStoreLocation();
	}

	/**
	 * clears the content of the cache entry corresponding to the storeID
	 * 
	 * @param storeID
	 * @throws NoSuchElementException
	 *             if {@link #assureContains(long)} does
	 */
	public void clearCacheEntry(long storeID) {
		assureContains(storeID);
		itemMap.get(storeID).setContent(null);
	}

	/**
	 * @param storeID
	 * @return index of the cache entry corresponding to the storeID
	 */
	public long getStoreIndex(long storeID) {
		assureContains(storeID);
		return itemMap.get(storeID).getDescription().getIndex();
	}

}
