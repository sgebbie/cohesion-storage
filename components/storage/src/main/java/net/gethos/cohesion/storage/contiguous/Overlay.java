/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Tracks memory allocations overlaying the
 * allocation tree.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
class Overlay {
	
	/**
	 * Maintained as a sorted minimal sequence of allocation intervals
	 * representing allocations and deallocations.
	 */
	private final List<AllocationRange> intervals;
	private final Comparator<Range> comparator;
	
	public Overlay() {
		this.comparator = new RangeComparator();
		this.intervals = new ArrayList<AllocationRange>();
	}
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		for (AllocationRange r : intervals) {
			s.append(r);
		}
		return s.toString();
	}
	
	public boolean isEmpty() {
		return intervals.isEmpty();
	}

	/**
	 * Overlay this range onto the current set of intervals.
	 * <p>
	 * This will correctly handle any intersections, and manage allocation status.
	 * Additionally, consecutive regions will be merged where possible.
	 * @param r
	 */
	public void record(AllocationRange r) {
		AllocationRange n = r.clone();
		Collection<Range> updated = new ArrayList<Range>();
		for (AllocationRange i : intervals) {
			if (i.allocated == n.allocated) {
				
				long istart = i.offset;
				long iend = i.length == Long.MAX_VALUE ? Long.MAX_VALUE : istart+i.length;
				long nstart = n.offset;
				long nend = n.length == Long.MAX_VALUE ? Long.MAX_VALUE : nstart + n.length;
				
				// check for intersection or touching
				if (!((iend <= n.offset) || (nend <= i.offset)) || istart == nend || nstart == iend) {
					// merge into new range
					nstart = Math.min(istart, nstart);
					nend = Math.max(iend, nend);
					n.offset = nstart;
					n.length = nend-nstart;
				} else {
					updated.add(i);
				}
			} else {
				if (i.intersects(n)) {
					Range.subtract(i, r, updated);
				} else {
					updated.add(i);
				}
			}
		}
		updated.add(n); // add the merged range
		
		intervals.clear();
		for(Range u : updated) intervals.add((AllocationRange)u);
		Collections.sort(intervals, comparator);
	}
	
	/**
	 * Clear a range from the current set of intervals.
	 * <p>
	 * This will correctly handle any intersections and compare allocation status.
	 * Additionally, consecutive regions will be merged where possible.
	 * @param r
	 */
	public void release(AllocationRange r) {
		Collection<Range> reduced = new ArrayList<Range>();
		reduced.add(r);
		Collection<Range> updated = new ArrayList<Range>();
		for (AllocationRange i : intervals) {
			if (!i.intersects(r)) {
				updated.add(i);
			} else {
				if (i.allocated == r.allocated) {
					reduced = Range.subtractAll(reduced, i);
					Range.subtract(i, r, updated);
				} else {
					updated.add(i);
					reduced = Range.subtractAll(reduced, i);
				}
			}
		}
		
		intervals.clear();
		for(Range u : reduced) intervals.add((AllocationRange)u);
		for(Range u : updated) intervals.add((AllocationRange)u);
		Collections.sort(intervals, comparator);
	}
	
	public AllocationRange peek() {
		if (intervals.isEmpty()) return null;
		return intervals.get(0);
	}
	
	public List<AllocationRange> allocations() {
		List<AllocationRange> allocations = new ArrayList<AllocationRange>();
		for(AllocationRange r : intervals) {
			if (r.allocated) allocations.add(r);
		}
		return allocations;
	}
	
	public List<AllocationRange> intervals() {
		return intervals;
	}
	
	private class RangeComparator implements Comparator<Range> {

		@Override
		public int compare(Range a, Range b) {
			if (a == b) return 0;
			if (a.offset == b.offset) return 0;
			if (a.offset < b.offset) return -1;
			return 1;
		}
		
	}
}
