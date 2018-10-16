package de.nuttercode.storm;

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
	 * the id of the item in the {@link de.nuttercode.storm.Store}
	 */
	private final long storeID;

	public StoreItem(long storeID, T content) {
		assert (content != null);
		this.storeID = storeID;
		this.content = content;
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
