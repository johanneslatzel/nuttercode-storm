package de.nuttercode.storm.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import de.nuttercode.storm.Store;
import de.nuttercode.storm.StoreConfiguration;
import de.nuttercode.util.Initializable;
import de.nuttercode.util.LongInterval;

/**
 * A component of a {@link Store}. manages available locations in the
 * {@link Store}. all locations are represented by {@link LongInterval}s with
 * absolute addresses in the DAF. {@link LongInterval#getBegin()} is the
 * begin-address of the location and {@link LongInterval#getEnd()} the
 * off-the-end-address. needs to be initialized by {@link #initialize(Set)}
 * before first usage.
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreLocationManager implements Initializable {

	/**
	 * available / free locations
	 */
	private final List<LongInterval> freeLocationList;

	/**
	 * reference to the {@link StoreFileManager}
	 */
	private final StoreFileManager storeFileManager;

	/**
	 * true if this component has been initialized.
	 */
	private boolean isInitialized;

	/**
	 * reference to the {@link StoreConfiguration} of the {@link Store}
	 */
	private final StoreConfiguration storeConfiguration;

	/**
	 * will be called by {@link Store}. don't call this constructor manually.
	 * 
	 * @param storeFileManager
	 * @param storeConfiguration
	 */
	public StoreLocationManager(StoreFileManager storeFileManager, StoreConfiguration storeConfiguration) {
		assert (storeConfiguration != null);
		assert (storeFileManager != null);
		this.storeFileManager = storeFileManager;
		this.storeConfiguration = new StoreConfiguration(storeConfiguration);
		freeLocationList = new ArrayList<>();
		isInitialized = false;
	}

	/**
	 * creates a new location from the given location with the given size. the
	 * remaining location will be saved in {@link #freeLocationList}.
	 * 
	 * @param location
	 * @param size
	 * @return a new location with the given size
	 */
	private LongInterval trim(LongInterval location, long size) {
		assert (size > 0);
		if (location.getLength() <= size)
			return location;
		LongInterval trimmedLocation = new LongInterval(location.getBegin(), location.getBegin() + size);
		freeLocationList.add(new LongInterval(trimmedLocation.getEnd(), location.getEnd()));
		return trimmedLocation;
	}

	/**
	 * finds and returns a location whose size is at least as given
	 * 
	 * @param size
	 * @return location whose size is at least as given
	 */
	private LongInterval find(long size) {
		int freeLocations;
		if (!freeLocationList.isEmpty()) {
			freeLocations = freeLocationList.size();
			for (int a = 0; a < freeLocations; a++) {
				if (freeLocationList.get(a).getLength() >= size) {
					return freeLocationList.remove(a);
				}
			}
		}
		return null;
	}

	/**
	 * assures that after this call at least one location with the given size exists
	 * and returns this location.
	 * 
	 * @param size
	 * @return a (new) free location with the given size
	 */
	private LongInterval assureFreeLocation(long size) {
		assert (size > 0);
		LongInterval foundLocation = find(size);
		if (foundLocation != null)
			return foundLocation;
		long actualSize = Math.max(size, storeConfiguration.getMinimumDataFileSize());
		return trim(storeFileManager.createNewStoreLocation(actualSize), size);
	}

	/**
	 * @param size
	 * @return a free location with the given size
	 */
	public LongInterval getFreeLocation(long size) {
		assert (size > 0);
		assert (isInitialized());
		return assureFreeLocation(size);
	}

	/**
	 * declares the given location as free
	 * 
	 * @param storeLocation
	 */
	public void addFreeLocation(LongInterval storeLocation) {
		assert (storeLocation != null);
		assert (isInitialized());
		freeLocationList.add(storeLocation);
	}

	/**
	 * initializes this component
	 * 
	 * @param initialStoreItemDescriptionSet
	 */
	public void initialize(Set<StoreItemDescription> initialStoreItemDescriptionSet) {

		assert (!isInitialized());

		// put into appropriate data structures
		SortedSet<LongInterval> reservedLocationSet = new TreeSet<>();
		for (StoreItemDescription d : initialStoreItemDescriptionSet) {
			reservedLocationSet.add(d.getStoreLocation());
		}

		// analyze data
		LongInterval splitLocation;
		long splitBegin;
		long reservedBegin;
		long splitEnd;
		long reservedEnd;
		long totalSpace = storeFileManager.getTotalSpace();
		if (totalSpace > 0)
			freeLocationList.add(new LongInterval(0, totalSpace));
		for (LongInterval reserved : reservedLocationSet) {
			splitLocation = null;
			for (LongInterval location : freeLocationList) {
				if (location.getBegin() <= reserved.getBegin() && reserved.getEnd() <= location.getEnd()) {
					splitLocation = location;
				}
			}
			if (splitLocation == null)
				throw new IllegalStateException();
			freeLocationList.remove(splitLocation);
			splitBegin = splitLocation.getBegin();
			reservedBegin = reserved.getBegin();
			reservedEnd = reserved.getEnd();
			splitEnd = splitLocation.getEnd();
			if (splitBegin < reservedBegin)
				freeLocationList.add(new LongInterval(splitBegin, reservedBegin));
			if (reservedEnd < splitEnd)
				freeLocationList.add(new LongInterval(reservedEnd, splitEnd));
		}

		isInitialized = true;

	}

	/**
	 * @return number of free bytes in the {@link Store}
	 */
	public long getFreeSpace() {
		long free = 0;
		for (LongInterval storeLocation : freeLocationList)
			free += storeLocation.getLength();
		return free;
	}

	/**
	 * @return number of free locations in the {@link Store}
	 */
	public int getFreeLocationCount() {
		return freeLocationList.size();
	}

	/**
	 * merges as many free locations as possible. this is a basic defragmentation
	 * process.
	 */
	public void mergeFreeLocations() {

		ArrayList<LongInterval> locationList = new ArrayList<>(freeLocationList);
		locationList.sort((left, right) -> {
			return Long.compare(left.getBegin(), right.getBegin());
		});

		LongInterval left, right;
		int leftIndex, rightIndex = 1;
		while (rightIndex < locationList.size()) {
			leftIndex = rightIndex - 1;
			left = locationList.get(leftIndex);
			right = locationList.get(rightIndex);
			if (left.getEnd() == right.getBegin()) {
				locationList.set(leftIndex, new LongInterval(left.getBegin(), right.getEnd()));
				locationList.remove(rightIndex);
			} else
				rightIndex++;
		}

		freeLocationList.clear();
		freeLocationList.addAll(locationList);
	}

	/**
	 * removes all tailing free locations and trims the DAF to the end of the last
	 * reserved location
	 * 
	 * @throws IOException when {@link StoreFileManager#setDataFileSize(long)} does
	 */
	public void trimDataFile() throws IOException {
		long end = storeFileManager.getTotalSpace();
		ArrayList<LongInterval> storeLocationList = new ArrayList<>(freeLocationList);
		storeLocationList.sort((left, right) -> {
			return Long.compare(right.getEnd(), left.getEnd());
		});
		LongInterval storeLocation;
		for (int a = 0; a < storeLocationList.size(); a++) {
			storeLocation = storeLocationList.get(a);
			if (storeLocation.getEnd() == end) {
				end = storeLocation.getBegin();
				freeLocationList.remove(storeLocation);
			} else
				break;
		}
		storeFileManager.setDataFileSize(end);
	}

	@Override
	public boolean isInitialized() {
		return isInitialized;
	}

}
