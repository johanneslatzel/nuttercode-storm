package de.nuttercode.storm;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import de.nuttercode.util.assurance.Assurance;
import de.nuttercode.util.assurance.NotNull;

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
 * @param <T> type of objects which will be stored in this store (content type)
 */
public interface Store<T> extends Closeable {

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
		return new StoreImpl<>(configuration, transformer);
	}

	/**
	 * deletes the item with the given id from this store
	 * 
	 * @param storeID
	 * @throws IOException
	 */
	void delete(long storeID) throws IOException;

	/**
	 * updates the content of the item in this store given by storeID
	 * 
	 * @param storeID
	 * @param content
	 * @throws IOException
	 */
	void update(long storeID, T content) throws IOException;

	/**
	 * @return a {@link StoreQuery} which can be used to query items in this store
	 */
	StoreQuery<T> query();

	/**
	 * @param storeID
	 * @return if an item stored in this store is identified by the given id
	 */
	boolean contains(long storeID);

	/**
	 * @param storeID
	 * @return content of the item stored in this store (identified by the given id)
	 * @throws IOException
	 */
	T getContent(long storeID) throws IOException;

	/**
	 * @param storeID
	 * @return a representation of the item stored in this store (identified by the
	 *         given id)
	 * @throws IOException
	 */
	StoreItem<T> get(long storeID) throws IOException;

	/**
	 * stores the content. reserves a new id and as much space as the content needs
	 * in the DAF.
	 * 
	 * @param content
	 * @return item
	 * @throws IOException
	 */
	StoreItem<T> store(T content) throws IOException;

	/**
	 * @return unmodifiable set of ids of every object stored
	 */
	Set<Long> getIds();

	/**
	 * @return true if no item is stored in this store
	 */
	boolean isEmpty();

	/**
	 * @return number of items stored in this store
	 */
	int size();

}
