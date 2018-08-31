package de.nuttercode.store.core;

/**
 * some location in the store - defined by its file and relative begin and (off
 * the) end positions
 * 
 * @author Johannes B. Latzel
 *
 */
public final class StoreLocation implements Comparable<StoreLocation> {

	private final long begin;
	private final long end;

	public StoreLocation(long begin, long end) {
		assert (begin < end);
		this.begin = begin;
		this.end = end;
	}

	public long getSize() {
		return end - begin;
	}

	public long getBegin() {
		return begin;
	}

	public long getEnd() {
		return end;
	}

	@Override
	public String toString() {
		return "StoreLocation [begin=" + begin + ", end=" + end + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (begin ^ (begin >>> 32));
		result = prime * result + (int) (end ^ (end >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StoreLocation other = (StoreLocation) obj;
		if (begin != other.begin)
			return false;
		if (end != other.end)
			return false;
		return true;
	}

	@Override
	public int compareTo(StoreLocation o) {
		return Long.compare(getSize(), o.getSize());
	}

}
