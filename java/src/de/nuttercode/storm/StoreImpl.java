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

	@Override
	public final void delete(long storeID) throws IOException {
		if (storeLog != null)
			storeLog.log("deleting item " + storeID);
		Index entry = indexMap.get(storeID);
		if (entry == null)
			throw new NoSuchElementException("no item with storeID: " + storeID);
		indexMap.remove(storeID);
		itemCache.remove(storeID);
		dataFile.free(entry);
	}

	@Override
	public final void update(long storeID, T content) throws IOException {
		if (storeLog != null)
			storeLog.log("updating item " + storeID);
		Index entry = indexMap.get(storeID);
		if (entry == null)
			throw new NoSuchElementException("no item with storeID: " + storeID);
		writableBuffer.clear();
		objectTransformer.putInto(content, writableBuffer);
		dataFile.free(entry);
		entry = dataFile.reserveSpace(storeID, readableBuffer.available());
		dataFile.writeData(entry, readableBuffer);
		itemCache.cache(storeID, content);
	}

	/**
	 * @return a {@link StoreQuery} which can be used to query items in this store
	 */
	@Override
	public StoreQuery<T> query() {
		return new StoreQuery<>(this, Collections.unmodifiableSet(indexMap.keySet()));
	}

	/**
	 * @param storeID
	 * @return if an item stored in this store is identified by the given id
	 */
	@Override
	public boolean contains(long storeID) {
		return indexMap.containsKey(storeID);
	}

	/**
	 * @param storeID
	 * @return content of the item stored in this store (identified by the given id)
	 * @throws IOException
	 */
	@Override
	public T getContent(long storeID) throws IOException {
		if (!itemCache.contains(storeID))
			cache(storeID);
		return itemCache.get(storeID);
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
		if (storeLog != null)
			storeLog.log("storing new content " + content);
		writableBuffer.clear();
		objectTransformer.putInto(content, writableBuffer);
		Index entry = dataFile.reserveSpace(readableBuffer.available());
		dataFile.writeData(entry, readableBuffer);
		itemCache.cache(entry.getId(), content);
		indexMap.put(entry.getId(), entry);
		return new StoreItem<>(this, entry.getId());
	}

	/**
	 * @return unmodifiable set of ids of every object stored
	 */
	@Override
	public Set<Long> getIds() {
		return Collections.unmodifiableSet(indexMap.keySet());
	}

	/**
	 * @return true if no item is stored in this store
	 */
	@Override
	public boolean isEmpty() {
		return indexMap.isEmpty();
	}

	/**
	 * @return number of items stored in this store
	 */
	@Override
	public int size() {
		return indexMap.size();
	}

	@Override
	public void close() throws IOException {
		if (storeLog != null)
			storeLog.log("closing store");
		dataFile.close();
		storeLog.close();
	}

}
