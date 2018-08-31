package de.nuttercode.storm.core;

import de.nuttercode.storm.StoreItem;

/**
 * This class represents a single entitity of a store. The content is not null
 * if and only if the content has been cached into RAM.
 * 
 * @author Johannes B. Latzel
 *
 * @param <T>
 *            type of content to cache
 */
public final class StoreCacheEntry<T> {

	/**
	 * some content - not null iff cached in RAM
	 */
	private T content;

	/**
	 * description of the content
	 */
	private final StoreCacheEntryDescription storeItemDescription;

	/**
	 * same as {@link #StoreCacheEntry(StoreCacheEntryDescription, Object)
	 * StoreCacheEntry(storeItemDescription, null)}
	 * 
	 * @param storeItemDescription
	 */
	public StoreCacheEntry(StoreCacheEntryDescription storeItemDescription) {
		this(storeItemDescription, null);
	}

	/**
	 * creates a new instance
	 * 
	 * @param storeItemDescription
	 * @param content
	 */
	public StoreCacheEntry(StoreCacheEntryDescription storeItemDescription, T content) {
		assert (storeItemDescription != null);
		this.storeItemDescription = storeItemDescription;
		this.content = content;
	}

	/**
	 * setter for {@link content}
	 * 
	 * @param content
	 */
	public void setContent(T content) {
		this.content = content;
	}

	/**
	 * returns a new {@link StoreItem} with this entries content and id
	 * 
	 * @return new {@link StoreItem}
	 */
	public StoreItem<T> createStoreItem() {
		return new StoreItem<T>(this);
	}

	/**
	 * getter for {@link storeItemDescription}
	 * 
	 * @return
	 */
	public StoreCacheEntryDescription getDescription() {
		return storeItemDescription;
	}

	/**
	 * @return iff content has been cached
	 */
	public boolean contentIsCached() {
		return content != null;
	}

	/**
	 * getter for {@link content}
	 * 
	 * @return {@link content}
	 * @throws IlelgalStateException
	 *             if content has not been cached
	 */
	public T getContent() {
		if (!contentIsCached())
			throw new IllegalStateException();
		return content;
	}

	@Override
	public String toString() {
		return "StoreCacheEntry [item=" + content + ", storeItemDescription=" + storeItemDescription + "]";
	}

}
