package de.nuttercode.store.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import de.nuttercode.store.StoreItem;

public final class StoreItemManager<T> {

	private final Map<Long, StoreCacheEntry<T>> itemMap;

	public StoreItemManager() {
		itemMap = new HashMap<>();
	}

	public boolean contains(long storeID) {
		return itemMap.containsKey(storeID);
	}

	public void newItem(StoreCacheEntryDescription storeItemDescription) {
		itemMap.put(storeItemDescription.getStoreID(), new StoreCacheEntry<>(storeItemDescription));
	}

	public void clearCache() {
		for (StoreCacheEntry<T> item : itemMap.values())
			item.setContent(null);
	}

	public SortedSet<Long> getStoreIDSet() {
		return new TreeSet<>(itemMap.keySet());
	}

	public StoreItem<T> get(long storeID) throws IOException {
		return itemMap.get(storeID).createStoreItem();
	}

	public void set(long storeID, StoreCacheEntry<T> item) {
		itemMap.put(storeID, item);
	}

	public void set(long storeID, T content) {
		itemMap.get(storeID).setContent(content);
	}

	public void remove(long storeID) {
		itemMap.remove(storeID);
	}

	public StoreLocation getStoreLocation(long storeID) {
		return itemMap.get(storeID).getDescription().getStoreLocation();
	}

	public void clearCacheEntry(long storeID) {
		itemMap.get(storeID).setContent(null);
	}

	public long getStoreIndex(long storeID) {
		return itemMap.get(storeID).getDescription().getIndex();
	}

}
