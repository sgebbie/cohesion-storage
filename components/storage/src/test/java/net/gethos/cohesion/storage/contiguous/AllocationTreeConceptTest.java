/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.BackedBTree;
import net.gethos.cohesion.storage.buffer.HeapBufferBacking;
import net.gethos.cohesion.storage.contiguous.AllocationMarker;

/**
 * Test the concepts in {@link ContainedBufferBacking} regarding the use of a
 * BTree for storing allocation information.
 *
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 * 
 */
public class AllocationTreeConceptTest {

	private BTree btree;

	@Before
	public void setUp() {
		btree = new BackedBTree(new HeapBufferBacking(20));
	}

	@Test
	public void regions() {

		AllocationContext actx = new AllocationContext();
		BTree.Key ml, mh;
		BTree.Reference r;
		long length, offset;
//		boolean ret;
		
		assertEquals(0,count(btree));

		// initialise by storing a 'alloc' and 'free' marker at the beginning
		// (note, alloc is less than free so this essentially marks the whole region as free)
		// [--------------------------------...)
		BTreeTransaction bt = btree.open();
		bt.truncate(AllocationMarker.ALLOCATED.key(0), 0);
		bt.truncate(AllocationMarker.FREE.key(0), 0);
		bt.commit();
		assertEquals(2,count(btree));
		
		// search for a free space of size x
		// [????????????????----------------...)
		offset = 0;
		length = 16;
		ml = findFree(btree, 0, length, actx);
		assertNotNull(actx.start);
		assertNull(actx.end); // therefore unbounded

		// allocate
		// [****************----------------...)
		allocate(btree, length, actx);
		bt = btree.open();
		r = bt.search(AllocationMarker.ALLOCATED.key(offset));
		assertNotNull(r);
		r = bt.search(AllocationMarker.FREE.key(offset + length));
		assertNotNull(r);
		assertEquals(2,count(btree));
		bt.close();
		
		// free a portion
		// [*****-----******----------------...)
		offset = 5;
		length = 5;
		findAllocated(btree, offset, length, actx);
//		ret = free(btree,offset,length,actx);
//		assertTrue(ret);
		
		bt = btree.open();
		bt.truncate(AllocationMarker.ALLOCATED.key(offset + length), 0);
		bt.truncate(AllocationMarker.FREE.key(offset), 0);
		bt.commit();
		assertEquals(4,count(btree));

		// search for a space larger than the gap
		// [*****-----******???????---------...)
		offset = 0;
		length = 7;
		ml = findFree(btree, offset, length, actx);
		assertNotNull(ml);
		assertEquals(16, ml.idHigh);

		// allocate larger gap
		// [*****-----*************---------...)
		offset = ml.idHigh;
		allocate(btree, length, actx);
		bt = btree.open();
		mh = bt.floor(AllocationMarker.MAX.key(Long.MAX_VALUE));
		bt.close();
		assertNotNull(mh);
		assertEquals(23,mh.idHigh);
		assertEquals(AllocationMarker.FREE.code,mh.type);
		assertEquals(4,count(btree));

		// search for a space smaller than the gap
		// [*****???--*************---------...)
		offset = 0;
		length = 3;
		ml = findFree(btree, offset, length, actx);
		assertNotNull(ml);
		assertEquals(5, ml.idHigh);

		// allocate smaller gap
		// [********--*************---------...)
		allocate(btree, length, actx);
		bt = btree.open();
		mh = bt.floor(AllocationMarker.MAX.key(Long.MAX_VALUE));
		bt.close();
		assertNotNull(mh);
		assertEquals(23,mh.idHigh);
		assertEquals(AllocationMarker.FREE.code,mh.type);
		assertEquals(4,count(btree));

		// search for a space equal to the tiny gap
		// [********??*************---------...)
		offset = 0;
		length = 2;
		ml = findFree(btree, offset, length, actx);
		assertNotNull(ml);
		assertEquals(8, ml.idHigh);
		assertNotNull(actx.start);
		assertNotNull(actx.end);
		assertEquals(length, actx.available);
		
		// allocate the tiny gap
		// [***********************---------...)
		allocate(btree, length, actx);
		
		// at the end the tree should have two entries
		bt = btree.open();
		r = bt.search(AllocationMarker.ALLOCATED.key(0));
		assertNotNull(r);
		r = bt.search(AllocationMarker.FREE.key(23));
		assertNotNull(r);
		bt.close();
		
		int count = count(btree);
		assertEquals(2,count);
	}
	
	private int count(BTree bt) {
		int count = 0;
		for (BTree.Key k : bt.open().range(BTree.Key.MIN_KEY, BTree.Key.MAX_KEY)) {
			if (k != null) count++;
		}
		return count;
	}
	
	/**
	 * Frees space from the tree, irrespective of whether or not it was
	 * previously allocated. That is, it need to be robust to various
	 * scenarios.
	 *  e.g. free a region that spans multiple allocation regions
	 */
	protected boolean free(BTree bt, long offset, long length, AllocationContext actx) {
		
		// make sure that the region we're freeing from is large enough
		if (actx == null || actx.available < length) return false;
		
		
//		 Strictly internal deallocation: insert new allocated start, insert new free start.
//		 Left internal deallocation: insert new allocated start, remove allocated start.
//		 Right internal deallocation: insert new free start, remove start of trailing free region.
//		 Complete deallocation: remove allocated start, remove start of trailing free region.
		
		return false;
	}

	/**
	 * FIXME make robust to allocate any region, whether previously free or allocated etc.
	 */
	private boolean allocate(BTree bt, long length, AllocationContext actx) {
		BTreeTransaction btt = bt.open();
		btt.delete(actx.start);
		if (length < actx.available) {
			// partial allocation
			btt.truncate(AllocationMarker.FREE.key(actx.start.idHigh + length), 0);	
		} else {
			// complete allocation
			if (actx.end != null) btt.delete(actx.end);
		}
		btt.commit();
		return true;
	}

	private static class AllocationContext {
		
//		public BTree.Key search;
		
		public BTree.Key start;
		public BTree.Key end;
		
		public long available;
		
		public void init(BTree.Key needle, BTree.Key s, BTree.Key e) {
//			this.search = needle;
			init(s,e);
		}
		
		public void init(BTree.Key s, BTree.Key e) {

			this.start = s;
			this.end = e;
			
			if (this.start != null) {
				this.available = this.end == null ? Long.MAX_VALUE : this.end.idHigh - this.start.idHigh;
			} else {
				this.available = -1;
			}
			
		}
		
		public static void init(AllocationContext actx, BTree.Key s, BTree.Key e) {
			if (actx == null) return;
			actx.init(s, e);
		}
	}
	
	/**
	 * In preparation for freeing a region, find
	 * the marker before and after the region in
	 * question.
	 * 
	 */
	private void findAllocated(BTree bt, long offset, long length, AllocationContext actx) {
		BTreeTransaction btt = bt.open();
		BTree.Key fs = AllocationMarker.FREE.key(offset);
		BTree.Key fn = AllocationMarker.MIN.key(offset+length);
		
		BTree.Key l = btt.floor(fs);
		BTree.Key h = btt.ceiling(fn);

		actx.init(fs, l, h);
		btt.close();
	}

	/**
	 * @return The key marking the start of the suitable free range, or null if
	 *         no range was found.
	 */
	private BTree.Key findFree(BTree bt, long offset, long length, AllocationContext actx) {

		BTree.Key f = null;
		BTree.Key a = null;
		
		// start searching the allocation tree at the given offset
		Iterator<BTree.Key> i = bt.open().range(AllocationMarker.MIN.key(offset), BTree.Key.MAX_KEY).iterator();

		while (i.hasNext()) {
			// find a free marker
			f = null;
			for (; i.hasNext();) {
				BTree.Key m = i.next();
				if (m.idx == AllocationMarker.ALLOCATION_IDX && m.type == AllocationMarker.FREE.code) {
					f = m;
					break;
				}
			}
			if (f == null) break; // ah, bugger :(

			// find the next allocated marker
			a = null;
			for (; i.hasNext();) {
				BTree.Key m = i.next();
				if (m.idx == AllocationMarker.ALLOCATION_IDX && m.type == AllocationMarker.ALLOCATED.code) {
					a = m;
					break;
				}
			}
			if (a == null) break; // Wahooo! the free range was unbounded... allocate as much as you want ;)

			// check the length
			long l = a.idHigh - f.idHigh;
			if (l >= length) break; // OK, good to go, just don't get carried away.
		}
		
		AllocationContext.init(actx, f, a);

		return f; 
	}

	@Test
	public void dequeRemove() {
		// remove elements from deque head/tail and check that the array is not copied around too much
		Deque<Integer> numbers = new ArrayDeque<Integer>();
		for (int i = 0; i < 10; i++) numbers.add(i);
		boolean ret = numbers.remove(0);
		assertTrue(ret);
		ret = numbers.remove(9);
		assertTrue(ret);
	}
}
