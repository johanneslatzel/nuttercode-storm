package de.nuttercode.storm.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import de.nuttercode.storm.StoreConfiguration;
import de.nuttercode.util.Initializable;

public final class StoreLocationManager implements Initializable {

	private final List<StoreLocation> freeLocationList;
	private final StoreFileManager storeFileManager;
	private boolean isInitialized;
	private final StoreConfiguration storeConfiguration;

	public StoreLocationManager(StoreFileManager storeFileManager, StoreConfiguration storeConfiguration) {
		assert (storeConfiguration != null);
		assert (storeFileManager != null);
		this.storeFileManager = storeFileManager;
		this.storeConfiguration = new StoreConfiguration(storeConfiguration);
		freeLocationList = new ArrayList<>();
		isInitialized = false;
	}

	private StoreLocation trim(StoreLocation location, long size) {
		assert (size > 0);
		if (location.getSize() <= size)
			return location;
		StoreLocation trimmedLocation = new StoreLocation(location.getBegin(), location.getBegin() + size);
		freeLocationList.add(new StoreLocation(trimmedLocation.getEnd(), location.getEnd()));
		return trimmedLocation;
	}

	private StoreLocation find(long size) {
		int freeLocations;
		StoreLocation currentLocation = null;
		if (!freeLocationList.isEmpty()) {
			freeLocations = freeLocationList.size();
			for (int a = 0; a < freeLocations; a++) {
				currentLocation = freeLocationList.get(a);
				if (currentLocation.getSize() >= size) {
					freeLocationList.remove(a);
					return currentLocation;
				}
			}
		}
		return null;
	}

	private StoreLocation assureFreeLocation(long size) {
		assert (size > 0);
		StoreLocation foundLocation = find(size);
		if (foundLocation != null)
			return foundLocation;
		long actualSize = Math.max(size, storeConfiguration.getMinimumDataFileSize());
		return storeFileManager.createNewStoreLocation(actualSize);
	}

	private void add(StoreLocation storeLocation) {
		freeLocationList.add(storeLocation);
	}

	public StoreLocation getFreeLocation(long size) {
		assert (size > 0);
		assureInitialized();
		return trim(assureFreeLocation(size), size);
	}

	public void addFreeLocation(StoreLocation storeLocation) {
		assert (storeLocation != null);
		assureInitialized();
		add(storeLocation);
	}

	public void initialize(Set<StoreCacheEntryDescription> initialStoreItemDescriptionSet) {

		assureUninitialized();

		// put into appropriate data structures
		SortedSet<StoreLocation> reservedLocationSet = new TreeSet<>();
		for (StoreCacheEntryDescription d : initialStoreItemDescriptionSet) {
			reservedLocationSet.add(d.getStoreLocation());
		}

		// analyze data
		StoreLocation splitLocation;
		long splitBegin;
		long reservedBegin;
		long splitEnd;
		long reservedEnd;
		long totalSpace = storeFileManager.getTotalSpace();
		if (totalSpace > 0)
			freeLocationList.add(new StoreLocation(0, totalSpace));
		for (StoreLocation reserved : reservedLocationSet) {
			splitLocation = null;
			for (StoreLocation location : freeLocationList) {
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
				freeLocationList.add(new StoreLocation(splitBegin, reservedBegin));
			if (reservedEnd < splitEnd)
				freeLocationList.add(new StoreLocation(reservedEnd, splitEnd));
		}

		isInitialized = true;

	}

	@Override
	public boolean isInitialized() {
		return isInitialized;
	}

	public long getFreeSpace() {
		long free = 0;
		for (StoreLocation storeLocation : freeLocationList)
			free += storeLocation.getSize();
		return free;
	}

	public int getFreeLocationCount() {
		return freeLocationList.size();
	}

	public void mergeFreeLocations() {

		ArrayList<StoreLocation> locationList = new ArrayList<>(freeLocationList);
		locationList.sort((left, right) -> {
			return Long.compare(left.getBegin(), right.getBegin());
		});

		StoreLocation left, right;
		int leftIndex, rightIndex = 1;
		while (rightIndex < locationList.size()) {
			leftIndex = rightIndex - 1;
			left = locationList.get(leftIndex);
			right = locationList.get(rightIndex);
			if (left.getEnd() == right.getBegin()) {
				locationList.set(leftIndex, new StoreLocation(left.getBegin(), right.getEnd()));
				locationList.remove(rightIndex);
			} else
				rightIndex++;
		}

		freeLocationList.clear();
		freeLocationList.addAll(locationList);
	}

	public void trimDataFile() throws IOException {
		long end = storeFileManager.getTotalSpace();
		ArrayList<StoreLocation> storeLocationList = new ArrayList<>(freeLocationList);
		storeLocationList.sort((left, right) -> {
			return Long.compare(right.getEnd(), left.getEnd());
		});
		StoreLocation storeLocation;
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

}
