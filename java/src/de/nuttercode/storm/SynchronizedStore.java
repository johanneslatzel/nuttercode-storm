package de.nuttercode.storm;

import java.io.IOException;
import java.util.Set;

import de.nuttercode.util.assurance.Assurance;
import de.nuttercode.util.assurance.NotNull;

/**
 * this wrapper assures that every operation is synchronized by one common
 * mutex. this class could be viewed as a decorator in the context of the
 * decorator pattern with an {@link ModifiableStore} as the base component.
 * 
 * @author Johannes B. Latzel
 *
 * @param <T> content type
 */
class SynchronizedStore<T> implements ModifiableStore<T> {

	/**
	 * the {@link Store} this instance wraps around
	 */
	private final ModifiableStore<T> store;

	/**
	 * mutex on which the {@link Store} synchronizes
	 */
	private final Object lock;

	SynchronizedStore(@NotNull ModifiableStore<T> store) {
		this.store = Assurance.assureNotNull(store);
		lock = new Object();
	}

	@Override
	public void close() throws IOException {
		synchronized (lock) {
			store.close();
		}
	}

	@Override
	public StoreQuery<T> query() {
		synchronized (lock) {
			return store.query();
		}
	}

	@Override
	public boolean contains(long storeID) {
		synchronized (lock) {
			return store.contains(storeID);
		}
	}

	@Override
	public T getContent(long storeID) throws IOException {
		synchronized (lock) {
			return store.getContent(storeID);
		}
	}

	@Override
	public StoreItem<T> get(long storeID) throws IOException {
		synchronized (lock) {
			return store.get(storeID);
		}
	}

	@Override
	public StoreItem<T> store(T content) throws IOException {
		synchronized (lock) {
			return new StoreItem<>(this, store.store(content).getId());
		}
	}

	@Override
	public Set<Long> getIds() {
		synchronized (lock) {
			return store.getIds();
		}
	}

	@Override
	public boolean isEmpty() {
		synchronized (lock) {
			return store.isEmpty();
		}
	}

	@Override
	public int size() {
		synchronized (lock) {
			return store.size();
		}
	}

	@Override
	public void delete(long storeID) throws IOException {
		synchronized (lock) {
			store.delete(storeID);
		}
	}

	@Override
	public void update(long storeID, T content) throws IOException {
		synchronized (lock) {
			store.update(storeID, content);
		}
	}

}
