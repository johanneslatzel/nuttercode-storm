package de.nuttercode.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import de.nuttercode.store.core.StoreItemManager;

/**
 * 
 * This class represents a query for items in a {@link de.nuttercode.store.Store}. The query
 * can be run as often as needed. Changes to the query can be made by calling
 * the intermediate functions.
 * 
 * @author Johannes B. Latzel
 *
 * @param <T>
 *            the type of the content of items in the {@link de.nuttercode.store.Store}
 */
public class StoreQuery<T> {

	private final List<Predicate<Long>> storeIDFilterList;
	private final List<Predicate<T>> contentFilterList;
	private final StoreItemManager<T> storeItemManager;
	private final Store<T> store;

	public StoreQuery(StoreItemManager<T> storeItemManager, Store<T> store) {
		assert (storeItemManager != null);
		assert (store != null);
		this.storeItemManager = storeItemManager;
		this.store = store;
		storeIDFilterList = new ArrayList<>(5);
		contentFilterList = new ArrayList<>(5);
	}

	private <H> boolean evaluateH(List<Predicate<H>> filterList, H h) {
		boolean test = true;
		for (int a = 0; a < filterList.size() && test; a++) {
			test &= filterList.get(a).test(h);
		}
		return test;
	}

	private boolean evaluateStoreID(long storeID) {
		return evaluateH(storeIDFilterList, storeID);
	}

	private boolean evaluateContent(T content) {
		return evaluateH(contentFilterList, content);
	}

	/**
	 * intermediate operation - filters items in respect to their storeID
	 * 
	 * @param storeIDFilter
	 * @return this
	 */
	public StoreQuery<T> whereID(Predicate<Long> storeIDFilter) {
		storeIDFilterList.add(storeIDFilter);
		return this;
	}

	/**
	 * intermediate operation - filters items in respect to their content
	 * 
	 * @param contentFilter
	 * @return this
	 */
	public StoreQuery<T> whereContent(Predicate<T> contentFilter) {
		contentFilterList.add(contentFilter);
		return this;
	}

	/**
	 * terminal operation - returns the first item which matches all filters
	 * 
	 * @return first item which matches all filters
	 * @throws IOException
	 *             when thrown by the {@link de.nuttercode.store.Store}
	 */
	public T first() throws IOException {
		T content;
		for (long storeID : storeItemManager.getStoreIDSet()) {
			if (!evaluateStoreID(storeID))
				break;
			content = store.get(storeID).getContent();
			if (evaluateContent(content))
				return content;
		}
		return null;
	}

	/**
	 * terminal operation - returns the last item which matches all filters
	 * 
	 * @return last item which matches all filters
	 * @throws IOException
	 *             when thrown by the {@link de.nuttercode.store.Store}
	 */
	public T last() throws IOException {
		T content;
		T last = null;
		for (long storeID : storeItemManager.getStoreIDSet()) {
			if (!evaluateStoreID(storeID))
				break;
			content = store.get(storeID).getContent();
			if (evaluateContent(content))
				last = content;
		}
		return last;
	}

	/**
	 * terminal operation - returns all items which match all filters
	 * 
	 * @return all items which match all filters
	 * @throws IOException
	 *             when thrown by the {@link de.nuttercode.store.Store}
	 */
	public Set<StoreItem<T>> all() throws IOException {
		Set<StoreItem<T>> itemSet = new HashSet<>();
		StoreItem<T> item;
		for (long storeID : storeItemManager.getStoreIDSet()) {
			if (evaluateStoreID(storeID)) {
				item = store.get(storeID);
				if (evaluateContent(item.getContent()))
					itemSet.add(item);
			}
		}
		return itemSet;
	}

}
