package de.nuttercode.storm;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import de.nuttercode.util.buffer.WritableBuffer;
import de.nuttercode.util.cache.Cache;
import de.nuttercode.util.cache.WeakCache;
import de.nuttercode.util.buffer.DataQueue;
import de.nuttercode.util.buffer.ReadableBuffer;

/**
 * an actual implementation of the {@link Store} interface. this could be viewed
 * as a concrete component in the context of the decorator pattern.
 * 
 * @author Johannes B. Latzel
 *
 * @param <T> content type
 */
class StoreImpl<T> implements ModifiableStore<T> {

	/**
	 * readable view of the same buffer {@link #writableBuffer} uses
	 */
	private final ReadableBuffer readableBuffer;

	/**
	 * writable view of the same buffer {@link #readableBuffer} uses
	 */
	private final WritableBuffer writableBuffer;

	/**
	 * transforms items of this store into binary format and vice versa
	 */
	private final ObjectTransformer<T> objectTransformer;

	/**
	 * cache of items stored in this store
	 */
	private final Cache<Long, T> itemCache;

	/**
	 * complete map of all ids of items stored in this store mapped to their
	 * corresponding indices
	 */
	private final Map<Long, Index> indexMap;

	/**
	 * used to log activities or null if logging is disabled
	 */
	private final StoreLog storeLog;

	/**
	 * DAF
	 */
	private final DataFile dataFile;

	/**
	 * log used to synchronize transactions if configuration specifies that this
	 * store needs to be thread safe
	 */
	private final Object storeLock;

	/**
	 * creates a new store
	 * 
	 * @param storeConfiguration
	 * @param objectTransformer
	 * @throws IOException
	 */
	StoreImpl(StoreConfiguration storeConfiguration, ObjectTransformer<T> objectTransformer) throws IOException {
		this.objectTransformer = objectTransformer;
		if (storeConfiguration.isLogEnabled()) {
			storeLog = new StoreLog(storeConfiguration.getLogFile());
			storeLog.log("initializing store");
		} else {
			storeLog = null;
		}
		storeLock = storeConfiguration.isThreadSafe() ? new Object() : null;
		dataFile = new DataFile(storeConfiguration, storeLog);
		indexMap = new HashMap<>();
		DataQueue buffer = new DataQueue();
		readableBuffer = buffer.readableView();
		writableBuffer = buffer.writableView();
		itemCache = new WeakCache<>();
		for (Index entry : dataFile.initialize())
			indexMap.put(entry.getId(), entry);
		if (storeLog != null)
			storeLog.log("done initializing");
	}

	/**
	 * loads the item with the given id from the DAF into the cache of this store
	 * 
	 * @param storeID
	 * @throws IOException
	 */
	private void cache(long storeID) throws IOException {
		if (storeLog != null)
			storeLog.log("caching item " + storeID);
		Index entry = indexMap.get(storeID);
		if (entry == null)
			throw new NoSuchElementException("no item with storeID: " + storeID);
		writableBuffer.clear();
		dataFile.readData(entry, writableBuffer);
		itemCache.cache(storeID, objectTransformer.getFrom(readableBuffer));
	}

	/**
	 * implementation of {@link #delete(long)}
	 * 
	 * @param storeId
	 * @throws IOException
	 */
	private void deleteImpl(long storeId) throws IOException {
		if (storeLog != null)
			storeLog.log("deleting item " + storeId);
		Index entry = indexMap.get(storeId);
		if (entry == null)
			throw new NoSuchElementException("no item with storeID: " + storeId);
		indexMap.remove(storeId);
		itemCache.remove(storeId);
		dataFile.free(entry);
	}

	/**
	 * implementation of {@link #update(long, Object)}
	 * 
	 * @param storeId
	 * @param content
	 * @throws IOException
	 */
	private void updateImpl(long storeId, T content) throws IOException {
		if (storeLog != null)
			storeLog.log("updating item " + storeId);
		Index entry = indexMap.get(storeId);
		if (entry == null)
			throw new NoSuchElementException("no item with storeID: " + storeId);
		writableBuffer.clear();
		objectTransformer.putInto(content, writableBuffer);
		dataFile.free(entry);
		entry = dataFile.reserveSpace(storeId, readableBuffer.available());
		dataFile.writeData(entry, readableBuffer);
		itemCache.cache(storeId, content);
	}

	/**
	 * implementation of {@link #contains(long)}
	 * 
	 * @param storeId
	 * @return
	 */
	private boolean containsImpl(long storeId) {
		return indexMap.containsKey(storeId);
	}

	/**
	 * implementation of {@link #getContentImpl(long)}
	 * 
	 * @param storeID
	 * @return
	 * @throws IOException
	 */
	private T getContentImpl(long storeID) throws IOException {
		if (!itemCache.contains(storeID))
			cache(storeID);
		return itemCache.get(storeID);
	}

	/**
	 * implementation of {@link #store(Object)}
	 * 
	 * @param content
	 * @return
	 * @throws IOException
	 */
	private StoreItem<T> storeImpl(T content) throws IOException {
		if (storeLog != null)
			storeLog.log("storing new content " + content);
		writableBuffer.clear();
		objectTransformer.putInto(content, writableBuffer);
		Index entry = dataFile.reserveSpace(readableBuffer.available());
		dataFile.writeData(entry, readableBuffer);
		itemCache.cache(entry.getId(), content);
		indexMap.put(entry.getId(), entry);
		return new StoreItem<T>(this, entry.getId());
	}

	/**
	 * implementation of {@link #getIds()}
	 * 
	 * @return
	 */
	private Set<Long> getIdsImpl() {
		return Collections.unmodifiableSet(indexMap.keySet());
	}

	/**
	 * implementation of {@link #isEmpty()}
	 * 
	 * @return
	 */
	private boolean isEmptyImpl() {
		return indexMap.isEmpty();
	}

	/**
	 * implementation of {@link #size()}
	 * 
	 * @return
	 */
	private int sizeImpl() {
		return indexMap.size();
	}

	/**
	 * implementation of {@link #close()}
	 * 
	 * @throws IOException
	 */
	private void closeImpl() throws IOException {
		if (storeLog != null)
			storeLog.log("closing store");
		dataFile.close();
		if (storeLog != null)
			storeLog.close();
	}

	@Override
	public final void delete(long storeID) throws IOException {
		if (storeLock != null) {
			synchronized (storeLock) {
				deleteImpl(storeID);
			}
		} else
			deleteImpl(storeID);
	}

	@Override
	public final void update(long storeID, T content) throws IOException {
		if (storeLock != null) {
			synchronized (storeLock) {
				updateImpl(storeID, content);
			}
		} else
			updateImpl(storeID, content);
	}

	/**
	 * @return a {@link StoreQuery} which can be used to query items in this store
	 */
	@Override
	public StoreQuery<T> query() {
		return new StoreQuery<>(this);
	}

	/**
	 * @param storeID
	 * @return if an item stored in this store is identified by the given id
	 */
	@Override
	public boolean contains(long storeID) {
		if (storeLock != null) {
			synchronized (storeLock) {
				return containsImpl(storeID);
			}
		} else
			return containsImpl(storeID);
	}

	/**
	 * @param storeID
	 * @return content of the item stored in this store (identified by the given id)
	 * @throws IOException
	 */
	@Override
	public T getContent(long storeID) throws IOException {
		if (storeLock != null) {
			synchronized (storeLock) {
				return getContentImpl(storeID);
			}
		} else
			return getContentImpl(storeID);
	}

	/**
	 * @param storeID
	 * @return a representation of the item stored in this store (identified by the
	 *         given id)
	 * @throws IOException
	 */
	@Override
	public StoreItem<T> get(long storeID) throws IOException {
		return new StoreItem<>(this, storeID);
	}

	/**
	 * stores the content. reserves a new id and as much space as the content needs
	 * in the DAF.
	 * 
	 * @param content
	 * @return item
	 * @throws IOException
	 */
	@Override
	public StoreItem<T> store(T content) throws IOException {
		if (storeLock != null) {
			synchronized (storeLock) {
				return storeImpl(content);
			}
		} else
			return storeImpl(content);
	}

	/**
	 * @return unmodifiable set of ids of every object stored
	 */
	@Override
	public Set<Long> getIds() {
		if (storeLock != null) {
			synchronized (storeLock) {
				return getIdsImpl();
			}
		} else
			return getIdsImpl();
	}

	/**
	 * @return true if no item is stored in this store
	 */
	@Override
	public boolean isEmpty() {
		if (storeLock != null) {
			synchronized (storeLock) {
				return isEmptyImpl();
			}
		} else
			return isEmptyImpl();
	}

	/**
	 * @return number of items stored in this store
	 */
	@Override
	public int size() {
		if (storeLock != null) {
			synchronized (storeLock) {
				return sizeImpl();
			}
		} else
			return sizeImpl();
	}

	@Override
	public void close() throws IOException {
		if (storeLock != null) {
			synchronized (storeLock) {
				closeImpl();
			}
		} else
			closeImpl();
	}

}
