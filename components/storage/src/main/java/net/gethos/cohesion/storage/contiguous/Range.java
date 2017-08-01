/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;


public class Range implements Comparable<Range> {

	public long offset;
	public long length;
	
	public Range() {
		this.offset = 0;
		this.length = 0;
	}
	
	public Range(long offset, long length) {
		assert(length >= 0);
		this.offset = offset;
		this.length = length;
	}
	
	protected Range newRange(long start, long end) {
		return $(start,end);
	}
	
	/**
	 * Creates a new Range representing [start,end).
	 * 
	 * @param start
	 * @param end
	 * @return create a new range from a half open range
	 */
	public static Range $(long start, long end) {
		return new Range(start,end == Long.MAX_VALUE ? Long.MAX_VALUE : end-start);
	}
	
	@Override
	public String toString() {
		return "[@" + offset + ":" + (length == Long.MAX_VALUE ? "inf" : Long.toString(length)) + "]";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (length ^ (length >>> 32));
		result = prime * result + (int) (offset ^ (offset >>> 32));
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (!(obj instanceof Range)) return false;
		Range other = (Range) obj;
		if (length != other.length) return false;
		if (offset != other.offset) return false;
		return true;
	}

	@Override
	public int compareTo(Range o) {
		if (this == o) return 0;
		if (o == null) return 1;
		if (offset == o.offset) {
			if (length == o.length) return 0;
			else return length < o.length ? -1 : 1;
		} else {
			return offset < o.offset ? -1 : 1;
		}
	}
	
	public boolean isEmpty() {
		return length == 0;
	}
	
	public boolean intersects(Range b) {
		return Range.intersects(this, b);
	}

	/**
	 * Check if two intervals intersect.
	 * 
	 * @param a
	 * @param r
	 * @return true if the two ranges intersect with each other
	 */
	public static boolean intersects(Range a, Range r) {
		long aEnd = a.length == Long.MAX_VALUE ? Long.MAX_VALUE : a.offset+a.length;
		long rEnd = r.length == Long.MAX_VALUE ? Long.MAX_VALUE : r.offset+r.length;
		if (rEnd <= a.offset) return false;
		if (aEnd <= r.offset) return false;
		return true;
	}

	/**
	 * Subtracts <code>r</code> from <code>a</code> and places
	 * the, possibly, new ranges into <code>result</code>.
	 * 
	 * @param a
	 * @param r
	 * @param result
	 */
	public static void subtract(Range a, Range r, Collection<? super Range> result) {
		if (!Range.intersects(a,r)) {
			result.add(a);
			return;
		} else {
			long aEnd = a.length == Long.MAX_VALUE ? Long.MAX_VALUE : a.offset+a.length;
			long rEnd = r.length == Long.MAX_VALUE ? Long.MAX_VALUE : r.offset+r.length;
			if (r.offset <= a.offset) {
				if (rEnd < aEnd) {
					// => r.offset <= a.offset <= rEnd < aEnd
					// [rrrrrraaa]
					result.add(a.newRange(rEnd,aEnd));
				} else {
					// => r.offset <= a.offset <= aEnd <= rEnd
					// [rrrrrrrrr] 
				}
			} else {
				// => a.offset < r.offset <= rEnd
				if (rEnd < aEnd) {
					// => a.offset < r.offset <= rEnd < aEnd
					// [aaarrrraaa]
					result.add(a.newRange(a.offset,r.offset));
					result.add(a.newRange(rEnd,aEnd));
				} else {
					// => a.offset < r.offset <= aEnd <= rEnd
					// [aaarrrrrrr]
					result.add(a.newRange(a.offset,r.offset));
				}
			}
		}
	}

	/**
	 * Remove multiple ranges from a set of ranges.
	 * 
	 * @param minuends
	 * @param subtrahends
	 * @return subranges remaining after removing all the <code>subtrahends</code>.
	 */
	public static Collection<Range> subtractAll(Collection<? extends Range> minuends, Collection<? extends Range> subtrahends) {
		Deque<Range> remainingCur = new ArrayDeque<Range>();
		remainingCur.addAll(minuends);
		
		return _subtractAll(remainingCur, subtrahends);
	}
	
	public static Collection<Range> subtractAll(Collection<? extends Range> minuends, Range subtrahend) {
		Deque<Range> remainingCur = new ArrayDeque<Range>();
		remainingCur.addAll(minuends);
		
		Deque<Range> subtrahends = new ArrayDeque<Range>();
		subtrahends.add(subtrahend);
		
		return _subtractAll(remainingCur, subtrahends);
	}
		
	public static Collection<Range> subtractAll(Range range, Collection<? extends Range> subtrahends) {
		Deque<Range> remainingCur = new ArrayDeque<Range>();
		remainingCur.add(range);
		
		return _subtractAll(remainingCur, subtrahends);
	}
	
	private static Collection<Range> _subtractAll(Collection<Range> remainingCur, Collection<? extends Range> subtrahends) {

		Collection<Range> remainingCarry = new ArrayDeque<Range>();
		
		for (Range a : subtrahends) {

			remainingCarry.clear();
			
			for (Range r : remainingCur) {
				Range.subtract(r,a,remainingCarry);
			}

			// swap
			Collection<Range> t = remainingCur;
			remainingCur = remainingCarry;
			remainingCarry = t;
		}
		
		return remainingCur;
	}

	/**
	 * Finds the union of two ranges.
	 * 
	 * @param a
	 * @param b
	 * @param r
	 * @return true if intersected
	 */
	public static boolean union(Range a, Range b, Collection<Range> r) {
		
		long aend = (a.offset + a.length);
		long bend = (b.offset + b.length);
		
		if (a.intersects(b) || a.offset == bend || b.offset == aend) {
			long start = a.offset < b.offset ? a.offset : b.offset;
			long end = aend < bend ? bend : aend;
			Range u = a.newRange(start, end);
			r.add(u);
			return true;
		} else {
			r.add(a);
			r.add(b);
			return false;
		}
	}

}
