/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.ReadOnlyTransactionBTree;
import net.gethos.cohesion.storage.TransactionBTree;
import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.NonCommittingDelegateBackingTransaction;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;
import net.gethos.cohesion.storage.buffer.ByteBufferNodeCapacities;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.heap.HeapCloneBacking;
import net.gethos.cohesion.storage.store.ByteBufferContiguousStore;
import net.gethos.cohesion.storage.store.ContiguousStore;
import static net.gethos.cohesion.storage.contiguous.AllocationMarkerTest.*;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class AllocationTreeTest {

	private BTreeBacking backing;
	
	@Before
	public void setUp() {
		backing = new HeapCloneBacking(4096);
		AllocationMarker.bootstrap(backing,0,0);
	}
	
	@Test
	public void searchEmpty() {
		BTreeBackingTransaction t = null;
		try {
			t = backing.open();
			final ReadOnlyTransactionBTree allocationTree = new ReadOnlyTransactionBTree(t);
			Range free = AllocationMarker.findFree(allocationTree, 0, 200);
			assertNotNull(free);
			assertEquals(0,free.offset);
			t.commit();
		} finally {
			if (t != null) t.close();
		}
	}
	
	@Test
	public void search() {
		BTreeBackingTransaction t = null;
		try {
			t = backing.open();
			
			// allocate various ranges
			BTreeTransaction wallocationTree = new TransactionBTree(t);
			assertTrue(AllocationMarker.allocRange(wallocationTree, 0, 60));
			assertTrue(AllocationMarker.freeRange(wallocationTree, 0,30));
			assertTrue(AllocationMarker.allocRange(wallocationTree, 5, 6));
			assertTrue(AllocationMarker.allocRange(wallocationTree, 7, 10));
			assertTrue(AllocationMarker.freeRange(wallocationTree, 6, 2));
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0),AllocationMarker.ALLOCATED.key(5),AllocationMarker.FREE.key(6),AllocationMarker.ALLOCATED.key(8),AllocationMarker.FREE.key(17),AllocationMarker.ALLOCATED.key(30),AllocationMarker.FREE.key(60));
			// [A:@000][F:@000][A:@005][F:@006][A:@008][F:@017][A:@030][F:@060]
			
			
			final ReadOnlyTransactionBTree allocationTree = new ReadOnlyTransactionBTree(t);
			
			Range r;
			r = AllocationMarker.findFree(allocationTree,0,10);
			assertNotNull(r);
			assertEquals(17,r.offset);
			assertEquals(13,r.length);
			
			r = AllocationMarker.findFree(allocationTree,0,2);
			assertNotNull(r);
			assertEquals(0,r.offset);
			assertEquals(5,r.length);
			
			r = AllocationMarker.findFree(allocationTree,4,2);
			assertNotNull(r);
			assertEquals(6,r.offset);
			assertEquals(2,r.length);
			
			r = AllocationMarker.findFree(allocationTree,0,13);
			assertNotNull(r);
			assertEquals(17,r.offset);
			assertEquals(13,r.length);
			
			r = AllocationMarker.findFree(allocationTree,0,20);
			assertNotNull(r);
			assertEquals(60,r.offset);
			assertEquals(Long.MAX_VALUE,r.length);
			
			r = AllocationMarker.findFree(allocationTree,80,40);
			assertNotNull(r);
			assertEquals(80,r.offset);
			assertEquals(Long.MAX_VALUE,r.length);
			
			t.commit();
		} finally {
			if (t != null) t.close();
		}
	}
	
	/**
	 * Test making updates to an allocation tree stored in
	 * a contiguous backed tree.
	 */
	@Test
	public void contiguousUpdate() {
		
		final ContiguousStore store = new ByteBufferContiguousStore(60000);
		final NodeCapacities nodeCapacities = new ByteBufferNodeCapacities();
		AllocationMarker.bootstrap(store, nodeCapacities);
		
		final long[] allocationPoints = new long[10];
		for (int i = 0; i < allocationPoints.length; i++) allocationPoints[i] = 4096*(i+1); // NB skip allocation of the header
		
		final BTreeBacking b = new BTreeBacking() {
			@Override
			public void close() {
				
			}
			
			@Override
			public BTreeBackingTransaction open() {
				return new TrivialContiguousTransaction(store, nodeCapacities, allocationPoints);
			}
			
			@Override
			public ReadOnlyBTreeBackingTransaction openReadOnly() {
				return new TrivialContiguousTransaction(store, nodeCapacities, allocationPoints);
			}
		};
		
		BTreeBackingTransaction t = null;
		try {
			t = b.open();
			AllocationMarker.bootstrap(t, 0, 4096*3); // two nodes and the header
			t.commit();
		} finally {
			if (t != null) t.close();
		}
		
		t = null;
		try {
			t = b.open();
			
			// allocate various ranges
			BTreeTransaction wallocationTree = new TransactionBTree(new NonCommittingDelegateBackingTransaction(t));

			assertTrue(AllocationMarker.allocRange(wallocationTree, 0,60));
			print(t,"contiguous-A");
			assertTrue(AllocationMarker.freeRange (wallocationTree, 0,30));
			assertTrue(AllocationMarker.allocRange(wallocationTree, 5, 6));
			assertTrue(AllocationMarker.allocRange(wallocationTree, 7,10));
			assertTrue(AllocationMarker.freeRange (wallocationTree, 6, 2));
			print(t,"contiguous-B");
			// [A:@000][F:@00000][A:@00005][F:@00006][A:@00008][F:@00017][A:@00030][F:@12288]
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0),AllocationMarker.ALLOCATED.key(5),AllocationMarker.FREE.key(6),AllocationMarker.ALLOCATED.key(8),AllocationMarker.FREE.key(17),AllocationMarker.ALLOCATED.key(30),AllocationMarker.FREE.key(12288));
			
			t.commit();
		} finally {
			if (t != null) t.close();
		}
	}

}
