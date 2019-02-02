package de.nuttercode.storm;

import java.io.IOException;

/**
 * 
 * represents some item in a {@link de.nuttercode.storm.Store}. Use
 * {@link #delete()} to delete this item. use {@link #update(Object)} to change
 * this items content. use {@link #exists()} to check if this item exists in the
 * corresponding {@link Store}.
 * 
 * @author Johannes B. Latzel
 *
 * @param <T> type of the content
 */
public final class StoreItem<T> {

	/**
	 * the id of the item in the {@link de.nuttercode.storm.Store}
	 */
	private final long storeID;

	/**
	 * {@link Store} in which this item is stored
	 */
	private final ModifiableStore<T> store;

	StoreItem(ModifiableStore<T> store, long storeID) {
		assert (store != null);
		this.storeID = storeID;
		this.store = store;
	}

	/**
	 * @return the id of the item in the {@link de.nuttercode.storm.Store}
	 */
	public long getId() {
		return storeID;
	}

	/**
	 * @return the content of the item
	 * @throws IOException
	 */
	public T getContent() throws IOException {
		return store.getContent(storeID);
	}

	/**
	 * updates the content of this item. this will be reflected in the corresponding
	 * {@link Store}.
	 * 
	 * @param content
	 * @throws IOException
	 */
	public void update(T content) throws IOException {
		store.update(getId(), content);
	}

	/**
	 * deletes this item from the corresponding {@link Store}. deleted items are
	 * invalid.
	 * 
	 * @throws IOException
	 */
	public void delete() throws IOException {
		store.delete(getId());
	}

	/**
	 * @return true if this item exists in the corresponding {@link Store}.
	 */
	public boolean exists() {
		return store.contains(getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((store == null) ? 0 : store.hashCode());
		result = prime * result + (int) (storeID ^ (storeID >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		StoreItem<T> other = (StoreItem<T>) obj;
		if (store == null) {
			if (other.store != null)
				return false;
		} else if (!store.equals(other.store))
			return false;
		if (storeID != other.storeID)
			return false;
		return true;
	}

	@Override
	public String toString() {
		Object content;
		try {
			content = getContent();
		} catch (IOException e) {
			content = e.getMessage();
		}
		return "StoreItem [getId()=" + getId() + ", getContent()=" + content + ", isValid()=" + exists() + "]";
	}

}
