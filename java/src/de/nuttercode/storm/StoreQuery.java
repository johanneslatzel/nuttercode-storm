package de.nuttercode.storm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * 
 * This class represents a query for {@link StoreItem}s in a
 * {@link de.nuttercode.storm.Store}. A query can be run as often as needed.
 * Changes to the {@link Store} will not be reflected in this query. This means
 * especially that changes to the {@link Store} may render this query invalid.
 * Changes to the query can be made by calling the intermediate functions.
 * 
 * @author Johannes B. Latzel
 *
 * @param <T> the type of the content of items in the
 *        {@link de.nuttercode.storm.Store}
 */
public class StoreQuery<T> {

	/**
	 * all filters on {@link StoreItem#getId()}
	 */
	private final List<Predicate<Long>> storeIDFilterList;

	/**
	 * all filters on {@link StoreItem#getContent()}
	 */
	private final List<Predicate<T>> contentFilterList;

	/**
	 * the {@link Store} on which this query will be run
	 */
	private final Store<T> store;

	/**
	 * called by {@link Store#query()}
	 * 
	 * @param store
	 * @param storeIDSet
	 */
	StoreQuery(Store<T> store) {
		assert (store != null);
		this.store = store;
		storeIDFilterList = new ArrayList<>();
		contentFilterList = new ArrayList<>();
	}

	/**
	 * tests if h satisfies the filterList
	 * 
	 * @param filterList
	 * @param h          some element
	 * @return true if h satisfies the filterList
	 */
	private <H> boolean evaluateH(List<Predicate<H>> filterList, H h) {
		boolean test = true;
		for (int a = 0; a < filterList.size() && test; a++) {
			test &= filterList.get(a).test(h);
		}
		return test;
	}

	/**
	 * @param storeID
	 * @return true if storeID satisfies the #storeIDFilterList
	 */
	private boolean evaluateStoreID(long storeID) {
		return evaluateH(storeIDFilterList, storeID);
	}

	/**
	 * @param content
	 * @return true if content satisfies the #contentFilterList
	 */
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
	 * terminal operation
	 * 
	 * @return a {@link Set} of all items which match all filters
	 * @throws IOException when thrown by the {@link de.nuttercode.storm.Store}
	 */
	public Set<StoreItem<T>> all() throws IOException {
		Set<StoreItem<T>> itemSet = new HashSet<>();
		StoreItem<T> item;
		for (long storeID : store.getIds()) {
			if (evaluateStoreID(storeID)) {
				item = store.get(storeID);
				if (evaluateContent(item.getContent()))
					itemSet.add(item);
			}
		}
		return itemSet;
	}

	/**
	 * terminal operation
	 * 
	 * @return {@link #all()} mapped to its {@link StoreItem#getContent()}
	 * @throws IOException when {@link #all()} does
	 */
	public Set<T> allContent() throws IOException {
		Set<T> set = new HashSet<>();
		for (StoreItem<T> item : all())
			set.add(item.getContent());
		return set;
	}

}
