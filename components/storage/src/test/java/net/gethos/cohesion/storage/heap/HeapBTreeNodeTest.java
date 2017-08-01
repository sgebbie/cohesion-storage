/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.backing.BTreeIndexNode;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapBTreeNodeTest {
	
	/**
	 * 
	 */
	private static final long DUMMY_CHILD_OFFSET_BASE = 10000000L;

	private static final int TEST_CAPACITY = 5;
	private static final int TEST_LENGTH = 20;
	
	private long keys[];
	private int indexes[];
	
	@Before
	public void setUp() {
		keys = new long[]{4, 3, 9, 5, 6, 8};
		indexes = new int[]{0, 0, 2, 2, 3, -5};
	}
	
	@Test
	public void drainFromLeft() {
		HeapLeafNode n = new HeapLeafNode(TEST_CAPACITY);
		HeapLeafNode sibling = new HeapLeafNode(TEST_CAPACITY);
		for (long k : new long[]{9}) n.realloc(key(k), TEST_LENGTH);
		for (long k : new long[]{3,5,4,6}) sibling.realloc(key(k), TEST_LENGTH);
		
		assertEquals(4,sibling.children());
		assertEquals(1,n.children());
		
//		System.out.printf("before s=%s%n", sibling);
//		System.out.printf("before n=%s%n", n);
		boolean ret = n.balance(sibling, false);
		assertTrue(ret);
//		System.out.printf("after s=%s%n", sibling);
//		System.out.printf("after n=%s%n", n);
		
		assertEquals(2,sibling.children());
		assertEquals(3,n.children());
		
		assertTrue(sibling.find(key(3)) >= 0);
		assertTrue(sibling.find(key(4)) >= 0);
		assertTrue(sibling.find(key(5)) < 0);
		assertTrue(sibling.find(key(6)) < 0);
		
		assertTrue(n.find(key(5)) >= 0);
		assertTrue(n.find(key(6)) >= 0);
		assertTrue(n.find(key(9)) >= 0);
	}
	
	@Test
	public void drainFromRight() {
		HeapLeafNode n = new HeapLeafNode(TEST_CAPACITY);
		HeapLeafNode sibling = new HeapLeafNode(TEST_CAPACITY);
		for (long k : new long[]{9}) n.realloc(key(k), TEST_LENGTH);
		for (long k : new long[]{11,17,13,19}) sibling.realloc(key(k), TEST_LENGTH);
		
		assertEquals(1,n.children());
		assertEquals(4,sibling.children());
		
		boolean ret = n.balance(sibling, false);
		assertTrue(ret);
		
		assertEquals(3,sibling.children());
		assertEquals(2,n.children());
		
		assertTrue(n.find(key(9)) >= 0);
		assertTrue(n.find(key(11)) >= 0);
		
		assertTrue(sibling.find(key(13)) >= 0);
		assertTrue(sibling.find(key(17)) >= 0);
		assertTrue(sibling.find(key(19)) >= 0);
	}
	
	@Test
	public void mergeToLeft() {
		HeapLeafNode n = new HeapLeafNode(TEST_CAPACITY);
		HeapLeafNode sibling = new HeapLeafNode(TEST_CAPACITY);
		for (long k : new long[]{9}) n.realloc(key(k), TEST_LENGTH);
		for (long k : new long[]{3,5,4,6}) sibling.realloc(key(k), TEST_LENGTH);
		
		assertEquals(1,n.children());
		assertEquals(4,sibling.children());
		
		boolean ret = n.balance(sibling, true);
		assertTrue(ret);
		
		assertEquals(5,sibling.children());
		assertEquals(0,n.children());
		
		assertTrue(sibling.find(key(3)) >= 0);
		assertTrue(sibling.find(key(4)) >= 0);
		assertTrue(sibling.find(key(5)) >= 0);
		assertTrue(sibling.find(key(6)) >= 0);
		assertTrue(sibling.find(key(9)) >= 0);
	}
	
	@Test
	public void mergeToRight() {
		HeapLeafNode n = new HeapLeafNode(TEST_CAPACITY);
		HeapLeafNode sibling = new HeapLeafNode(TEST_CAPACITY);
		for (long k : new long[]{9}) n.realloc(key(k), TEST_LENGTH);
		for (long k : new long[]{11,17,13}) sibling.realloc(key(k), TEST_LENGTH);
		
		assertEquals(1,n.children());
		assertEquals(3,sibling.children());
		
		boolean ret = n.balance(sibling, true);
		assertTrue(ret);
		
		assertEquals(4,sibling.children());
		assertEquals(0,n.children());
		
		assertTrue(sibling.find(key( 9)) >= 0);
		assertTrue(sibling.find(key(11)) >= 0);
		assertTrue(sibling.find(key(13)) >= 0);
		assertTrue(sibling.find(key(17)) >= 0);
	}
	
	@Test
	public void deleteIndexRight() {
		HeapIndexNode n = new HeapIndexNode(TEST_CAPACITY);
		for (long k : new long[]{2,9,1,4}) storeIndex(n, key(k), DUMMY_CHILD_OFFSET_BASE + k);
		assertEquals(4, n.size);
		assertEquals(BTreeIndexNode.INVALID_OFFSET,n.rightChild);
		long rdummy = 5678L;
		n.rightChild = rdummy;
		
		// delete left
		boolean ret = n.delete(2);
		assertTrue(ret);
		assertEquals(3,n.size);
		assertEquals(rdummy,n.rightChild);
		
		// try delete past end
		ret = n.delete(n.size+1);
		assertFalse(ret);
		assertEquals(rdummy,n.rightChild);
		assertEquals(3,n.size);
		
		// delete right-hand-child
		ret = n.delete(n.size);
		assertTrue(ret);
		assertEquals(DUMMY_CHILD_OFFSET_BASE + 9,n.rightChild);
		assertEquals(2,n.size);
		
		// delete right-hand-item
		ret = n.delete(n.size-1);
		assertTrue(ret);
		assertEquals(DUMMY_CHILD_OFFSET_BASE + 9,n.rightChild);
		assertEquals(1,n.size);
	}

	@Test
	public void deleteLeafItems() {
		for (int i = 0; i < keys.length-1;i++) {
			deleteLeafItems(keys[i]);
		}
	}
	
	public void deleteLeafItems(long id) {
		HeapLeafNode n = new HeapLeafNode(TEST_CAPACITY);
		storeLeafItems(n);
		
		// now delete an item and check that we can't find it again
		BTree.Key k = key(id);
		int idx = n.find(k);
		assertTrue(idx >= 0);
		assertTrue(idx < n.children());
		
		boolean ret = n.delete(idx);
		assertTrue(ret);
		
		int idxAgain = n.find(k);
		assertEquals(-idx-1,idxAgain);
	}

	@Test
	public void storeLeafItems() {
		
		// store a sequence of keys (4,3,9,5,6,8)
		// should result in (3,4,5,6,9) and the '8' won't be stored because the node is full
		
		HeapLeafNode n = new HeapLeafNode(TEST_CAPACITY);
		
		storeLeafItems(n);
	}
	
	private void storeLeafItems(HeapLeafNode n) {
		
		// store a sequence of keys (4,3,9,5,6,8)
		// should result in (3,4,5,6,9) and the '8' won't be stored because the node is full
		
		for (int i = 0; i < keys.length; i++) {
			int x = n.realloc(key(keys[i]), TEST_LENGTH);
			assertEquals(indexes[i], x);
		}
		
	}
	
	@Test
	public void findLeafItems() {
		
		// populate the node
		HeapLeafNode n = new HeapLeafNode(TEST_CAPACITY);
		for (int i = 0; i < keys.length; i++) {
			int x = n.realloc(key(keys[i]), TEST_LENGTH);
			assertEquals(indexes[i], x);
		}
		
		// now check that we can find things
		BTree.Key k7 = key(7);
		int x = n.find(k7);
		assertTrue(x < 0);
		int ip = -x-1;
		assertTrue(ip>=0);
		assertEquals(4,ip);
	}
	
	@Test
	public void findEmptyLeafItems() {
		
		// populate the node
		HeapLeafNode n = new HeapLeafNode(TEST_CAPACITY);
		
		BTree.Key k7 = key(7);
		int x = n.find(k7);
		assertEquals(-1, x);
		int ip = -x-1;
		assertEquals(0, ip);
	}
	
	@Test
	public void splitLeft() {
		long split = 4;
		long[] initialIds = new long[] {3,5,7,11,13};
		long[] siblingUpdated = new long[] { 3, 4, 5 };
		long[] initialUpdated = new long[] { 7,11,13 };
		long[] parentUpdated  = new long[] { 5 };
		
		doSplit(split, initialIds, initialUpdated, siblingUpdated, parentUpdated);
	}
	
	@Test
	public void splitRight() {
		// note, there is a slight bias here because after the drain and store we don't end up with a balanced output
		long split = 12;
		long[] initialIds = new long[] {3,5,7,11,13};
		long[] siblingUpdated = new long[] { 3, 5 };
		long[] initialUpdated = new long[] { 7, 11, 12, 13 };
		long[] parentUpdated  = new long[] { 5 };
		
		doSplit(split, initialIds, initialUpdated, siblingUpdated, parentUpdated);
	}
	
	@Test
	public void splitBoundary() {
		long split = 6;
		long[] initialIds = new long[] {3,5,7,11,13};
		long[] siblingUpdated = new long[] { 3, 5 };
		long[] initialUpdated = new long[] { 6, 7, 11, 13 };
		long[] parentUpdated  = new long[] { 5 };
		
		doSplit(split, initialIds, initialUpdated, siblingUpdated, parentUpdated);
	}
	
	@Test(expected=IllegalStateException.class)
	public void splitExists() {
		long split = 11;
		long[] initialIds = new long[] {3,5,7,11,13};
		long[] initialUpdated = new long[] {3,5,7,11,13};
		long[] siblingUpdated = new long[] {};
		long[] parentUpdated  = new long[] {};
		
		doSplit(split, initialIds, initialUpdated, siblingUpdated, parentUpdated);
	}
	
	private void doSplit(long split, long[] initialIds, long[] initialUpdated, long[] siblingUpdated, long[] parentUpdated) {
		
		long dummyOffset = 123456;
		
		HeapIndexNode parent = new HeapIndexNode(TEST_CAPACITY);
		HeapLeafNode initial = new HeapLeafNode(TEST_CAPACITY);
		HeapLeafNode sibling = new HeapLeafNode(TEST_CAPACITY);
		
		// populate the initial node so that it is full
		for (long i : initialIds) initial.realloc(key(i), TEST_LENGTH);
		assertEquals(initialIds.length, initial.size);
		assertEquals(0,parent.size);
		assertEquals(0,sibling.size);
		
		//split the node into two
		BTree.Key s = key(split);
		split(initial, sibling, s);
	
		// create the new parent item and insert
		int ri = storeIndex(parent,sibling.item(sibling.size-1).key, dummyOffset);
		assert(ri >= 0);
		
		// print split
//		System.out.println("split=" + split);
//		System.out.println("initial=" + Arrays.toString(initialIds));
//		System.out.println("expected initialUpdated=" + Arrays.toString(initialUpdated) + " was=" + keyList(initial));
//		System.out.println("expected siblingUpdated=" + Arrays.toString(siblingUpdated) + " was=" + keyList(sibling));
//		System.out.println("expected parentUpdated=" + Arrays.toString(parentUpdated) + " was=" + keyList(parent));
		
		// now check the split
		assertEquals(initialUpdated.length, initial.size);
		for (int i = 0; i < initialUpdated.length; i++) assertEquals(i,initial.find(key(initialUpdated[i])));
		
		assertEquals(siblingUpdated.length, sibling.size);
		for (int i = 0; i < siblingUpdated.length; i++) assertEquals(i,sibling.find(key(siblingUpdated[i])));
		
		assertEquals(parentUpdated.length, parent.size);
		for (int i = 0; i < parentUpdated.length; i++) assertEquals(i,parent.find(key(parentUpdated[i])));
	}

	protected String keyList(BTreeNode n) {
		StringBuilder s = new StringBuilder();
		s.append("[");
		String sep = "";
		for(int i = 0; i < n.children(); i++) {
			s.append(sep);
			s.append(n.key(i).idHigh);
			sep = ", ";
		}
		s.append("]");
		return s.toString();
	}

	private void split(HeapLeafNode initial, HeapLeafNode sibling, BTree.Key s) {
		// perform node split
		int ri = initial.find(s);
		if (ri >= 0) throw new IllegalStateException("can not split this index node as the key: " + s + " already exists");
		sibling.balance(initial, false);
		if (s.compareTo(sibling.rightHandKey()) > 0) {
			initial.realloc(s,0);
		} else {
			sibling.realloc(s,0);
		}
	}

	private BTree.Key key(long id) {
		BTree.Key k = new BTree.Key();
		k.idx = 1;
		k.idHigh = id;
		return k;
	}

	private int storeIndex(HeapIndexNode n, BTree.Key key, long childOffset) {
		int idx = n.alloc(key);
		n.write(idx, childOffset);
		return idx;
	}
	
}

