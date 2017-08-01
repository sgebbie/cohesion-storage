/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class AllocationRange extends Range implements Comparable<Range>, Cloneable {
	
	public boolean allocated;
	
	public AllocationRange(long offset, long length, boolean allocated) {
		super(offset,length);
		this.allocated = allocated;
	}
	
	@Override
	public AllocationRange clone() {
		try {
			AllocationRange r = (AllocationRange) super.clone();
			return r;
		} catch (CloneNotSupportedException e) {
			throw new AssertionError(e);
		}
	}
	
	@Override
	protected Range newRange(long start, long end) {
		return new AllocationRange(start,end == Long.MAX_VALUE ? Long.MAX_VALUE : end-start,allocated);
	}
	
	@Override
	public String toString() {
		return "[@" + offset + ":" + length + ":" + (allocated?"A":"F") + "]";
	}
	
	@Override
	public int compareTo(Range o) {
		if (o instanceof AllocationRange) {
			AllocationRange r = (AllocationRange)o;
			if (allocated != r.allocated) {
				return allocated ? 0 : 1;
			}
		}
		return super.compareTo(o);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (allocated ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof AllocationRange)) return false;
		AllocationRange other = (AllocationRange) obj;
		if (allocated != other.allocated) return false;
		return true;
	}
}
