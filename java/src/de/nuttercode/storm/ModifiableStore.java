package de.nuttercode.storm;

import java.io.IOException;

/**
 * 
 * extends the {@link Store} with {@link #update(long, Object)} and
 * {@link #delete(long)} methods. those methods are used internally and could
 * (but should not) be used by the user. {@link StoreItem#update(Object)} and
 * {@link StoreItem#delete()} should be used instead. this interface could be
 * viewed as the base component in the context of the decorator pattern.
 * 
 * @author Johannes B. Latzel
 *
 * @param <T> content type
 */
interface ModifiableStore<T> extends Store<T> {

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

}
