package de.nuttercode.storm;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import de.nuttercode.util.buffer.WritableBuffer;
import de.nuttercode.util.cache.Cache;
import de.nuttercode.util.cache.WeakCache;
import de.nuttercode.util.assurance.Assurance;
import de.nuttercode.util.assurance.NotNull;
import de.nuttercode.util.buffer.DataQueue;
import de.nuttercode.util.buffer.ReadableBuffer;

/**
 * A {@link Store} saves objects (as items with an unique id) into a persistent
 * format (a file). {@link StoreConfiguration}s are used to configure a
 * {@link Store} instance. {@link ObjectTransformer} are used to transform
 * objects into binary format and vice versa. Either use the provided
 * transformers in this package (e.g. {@link SerializableTransformer}) or create
 * your own by implementing {@link ObjectTransformer}. Use
 * {@link Store#open(StoreConfiguration)} and
 * {@link Store#open(StoreConfiguration, ObjectTransformer)} to open a
 * {@link Store}. Be careful to always {@link #close()} an instance. Instances
 * which are not closed properly may be invalid (especially the persistent data
 * may be corrupted). Use {@link #get(long)}, {@link #query()}, and
 * {@link #getContent(long)} to read objects. Use {@link #contains(long)},
 * {@link #size()}, and {@link #isEmpty()} to check for the existence of items.
 * See {@link StoreItem} for interactions with items.
 * 
 * @author Johannes B. Latzel
 *
 * @param <T> type of objects which will be stored in this store
 */
public class Store<T> implements Closeable {

	/**
	 * opens a {@link Store} of an serializable class.
	 * {@link SerializableTransformer} will be used as the transformer.
	 * 
	 * @param configuration
	 * @return {@link Store#open(StoreConfiguration, ObjectTransformer)
	 *         Store.open(configuration, new SerializableTransformer<>())}
	 * @throws IOException when {@link #open(StoreConfiguration, ObjectTransformer)}
	 *                     does
	 */
	public static <T extends Serializable> Store<T> open(@NotNull StoreConfiguration configuration) throws IOException {
		return Store.open(configuration, new SerializableTransformer<>());
	}

	/**
	 * opens a {@link Store}. provide an appropriate configuration and transformer.
	 * a store can not be reopened until is was closed properly.
	 * 
	 * @param configuration
	 * @param transformer
	 * @return {@link Store}
	 * @throws IOException
	 */
	public static <T> Store<T> open(@NotNull StoreConfiguration configuration,
			@NotNull ObjectTransformer<T> transformer) throws IOException {
		Assurance.assureNotNull(configuration);
		Assurance.assureNotNull(transformer);
		return new Store<>(configuration, transformer);
	}

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
	private Store(StoreConfiguration storeConfiguration, ObjectTransformer<T> objectTransformer) throws IOException {
		this.objectTransformer = objectTransformer;
		dataFile = new DataFile(storeConfiguration);
		indexMap = new HashMap<>();
		DataQueue buffer = new DataQueue();
		readableBuffer = buffer.readableView();
		writableBuffer = buffer.writableView();
		itemCache = new WeakCache<>();
		for (Index entry : dataFile.initialize())
			indexMap.put(entry.getId(), entry);
	}

	/**
	 * loads the item with the given id from the DAF into the cache of this store
	 * 
	 * @param storeID
	 * @throws IOException
	 */
	private void cache(long storeID) throws IOException {
		Index entry = indexMap.get(storeID);
		if (entry == null)
			throw new NoSuchElementException("no item with storeID: " + storeID);
		writableBuffer.clear();
		dataFile.readData(entry, writableBuffer);
		itemCache.cache(storeID, objectTransformer.getFrom(readableBuffer));
	}

	/**
	 * deletes the item with the given id from this store
	 * 
	 * @param storeID
	 * @throws IOException
	 */
	final void delete(long storeID) throws IOException {
		Index entry = indexMap.get(storeID);
		if (entry == null)
			throw new NoSuchElementException("no item with storeID: " + storeID);
		indexMap.remove(storeID);
		itemCache.remove(storeID);
		dataFile.free(entry);
	}

	/**
	 * updates the content of the item in this store given by storeID
	 * 
	 * @param storeID
	 * @param content
	 * @throws IOException
	 */
	final void update(long storeID, T content) throws IOException {
		Index entry = indexMap.get(storeID);
		long dataLength;
		if (entry == null)
			throw new NoSuchElementException("no item with storeID: " + storeID);
		writableBuffer.clear();
		objectTransformer.putInto(content, writableBuffer);
		dataLength = readableBuffer.available();
		if (entry.getDataLocation().getLength() != dataLength) {
			dataFile.free(entry);
			entry = dataFile.reserveSpace(storeID, dataLength);
		}
		dataFile.writeData(entry, readableBuffer);
		itemCache.cache(storeID, content);
	}

	/**
	 * @return a {@link StoreQuery} which can be used to query items in this store
	 */
	public StoreQuery<T> query() {
		return new StoreQuery<>(this, Collections.unmodifiableSet(indexMap.keySet()));
	}

	/**
	 * @param storeID
	 * @return if an item stored in this store is identified by the given id
	 */
	public final boolean contains(long storeID) {
		return indexMap.containsKey(storeID);
	}

	/**
	 * @param storeID
	 * @return content of the item stored in this store (identified by the given id)
	 * @throws IOException
	 */
	public final T getContent(long storeID) throws IOException {
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
	public final StoreItem<T> get(long storeID) throws IOException {
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
	public final StoreItem<T> store(T content) throws IOException {
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
	public final Set<Long> getIds() {
		return Collections.unmodifiableSet(indexMap.keySet());
	}

	/**
	 * @return true if no item is stored in this store
	 */
	public boolean isEmpty() {
		return indexMap.isEmpty();
	}

	/**
	 * @return number of items stored in this store
	 */
	public int size() {
		return indexMap.size();
	}

	@Override
	public final void close() throws IOException {
		dataFile.close();
	}

}
