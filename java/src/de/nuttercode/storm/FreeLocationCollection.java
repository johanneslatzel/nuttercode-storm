package de.nuttercode.storm;

import java.util.TreeSet;

import de.nuttercode.util.LongInterval;
import de.nuttercode.util.Range;

/**
 * saves and manages all free locations in the DAF
 * 
 * @author Johannes B. Latzel
 *
 */
class FreeLocationCollection {

	/**
	 * all free locations ordered by length (begin is tie breaker)
	 */
	private final TreeSet<LongInterval> freeSetByLength;

	/**
	 * all free locations ordered by length (no tie breaker needed since two
	 * locations with same begin would overlap)
	 */
	private final TreeSet<LongInterval> freeSetByBegin;

	FreeLocationCollection() {
		freeSetByLength = new TreeSet<>((l, r) -> {
			int cmp = Long.compare(l.getLength(), r.getLength());
			return cmp != 0 ? cmp : Long.compare(l.getBegin(), r.getBegin());
		});
		freeSetByBegin = new TreeSet<>((l, r) -> Long.compare(l.getBegin(), r.getBegin()));
	}

	/**
	 * removes the location from this collection
	 * 
	 * @param location
	 */
	private void remove(LongInterval location) {
		if (!freeSetByLength.contains(location))
			throw new IllegalStateException("location " + location + " is not free");
		if (!freeSetByLength.remove(location))
			throw new IllegalStateException("location " + location + " could not be removed");
		if (!freeSetByBegin.contains(location))
			throw new IllegalStateException("location " + location + " is not free");
		if (!freeSetByBegin.remove(location))
			throw new IllegalStateException("location " + location + " could not be removed");
	}

	/**
	 * adds the location to this collection
	 * 
	 * @param location
	 */
	void add(LongInterval location) {
		if (freeSetByLength.contains(location))
			throw new IllegalStateException("location " + location + " is already free");
		if (!freeSetByLength.add(location))
			throw new IllegalStateException("location " + location + " could not be added");
		if (freeSetByBegin.contains(location))
			throw new IllegalStateException("location " + location + " is already free");
		if (!freeSetByBegin.add(location))
			throw new IllegalStateException("location " + location + " could not be added");
	}

	/**
	 * marks the location as reserved and therefore not-free
	 * 
	 * @param location
	 */
	void reserve(LongInterval location) {
		LongInterval free = freeSetByBegin.floor(location);
		if (free == null)
			throw new IllegalArgumentException("no free element containing " + location + " available");
		if (!free.contains(location))
			throw new IllegalArgumentException(free + " does not contain " + location);
		remove(free);
		if (free.getBegin() != location.getBegin())
			add(Range.of(free.getBegin(), location.getBegin()));
		if (location.getEnd() != free.getEnd())
			add(Range.of(location.getEnd(), free.getEnd()));
	}

	/**
	 * searches for a free location of at least size bytes length. if such a
	 * location exists then this method will removed it from this collection and
	 * return the location. if not {@code null} will be returned.
	 * 
	 * @param size >= 1 (not checked)
	 * @return a location of length >= size or (if it does not exist) {@code null}
	 */
	LongInterval remove(long size) {
		LongInterval free = freeSetByLength.ceiling(Range.of(0, size));
		if (free != null)
			remove(free);
		return free;
	}

}
