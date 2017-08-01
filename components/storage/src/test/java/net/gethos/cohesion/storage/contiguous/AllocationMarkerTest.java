/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.BackedBTree;
import net.gethos.cohesion.storage.TransactionBTree;
import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.NonCommittingDelegateBackingTransaction;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.ReadOnlyDelegateBackingTransaction;
import net.gethos.cohesion.storage.contiguous.AllocationMarker;
import net.gethos.cohesion.storage.heap.HeapCloneBacking;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class AllocationMarkerTest {

	private BTreeBacking backing;
	
	@Before
	public void setUp() {
		backing = new HeapCloneBacking(4096);
		AllocationMarker.bootstrap(backing,0,0);
	}
	
	@Test
	public void ranges() {
		BTreeBackingTransaction t = null;
		try {
			t = backing.open();
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0));
//			print(BTree.Key.MIN_KEY, BTree.Key.MAX_KEY, t, "(-inf,+inf)");
//			print(AllocationMarker.MIN.key(0), AllocationMarker.MIN.key(60), t,"0,60");	
		} finally {
			if (t != null) t.close();
		}
	}
	
	@Test
	public void updates() {
		BTreeBackingTransaction t = null;
		try {
			boolean ret;
			t = backing.open();
			final BTreeTransaction at = new TransactionBTree(t);
			
			print(t,"Start...");
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0));
			
			ret = AllocationMarker.allocRange(at, 0, 60);
			print(t,"alloc(0,60)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(60));
			
			ret = AllocationMarker.freeRange(at, 0,30);
			print(t,"free(0,30)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0),AllocationMarker.ALLOCATED.key(30),AllocationMarker.FREE.key(60));
			
			ret = AllocationMarker.allocRange(at, 5, 6);
			print(t,"alloc(5,6)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0),AllocationMarker.ALLOCATED.key(5),AllocationMarker.FREE.key(11),AllocationMarker.ALLOCATED.key(30),AllocationMarker.FREE.key(60));
			
			ret = AllocationMarker.allocRange(at, 7, 10);
			print(t,"alloc(7,10)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0),AllocationMarker.ALLOCATED.key(5),AllocationMarker.FREE.key(17),AllocationMarker.ALLOCATED.key(30),AllocationMarker.FREE.key(60));
			
			ret = AllocationMarker.freeRange(at, 6, 2);
			print(t,"free(6,2)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0),AllocationMarker.ALLOCATED.key(5),AllocationMarker.FREE.key(6),AllocationMarker.ALLOCATED.key(8),AllocationMarker.FREE.key(17),AllocationMarker.ALLOCATED.key(30),AllocationMarker.FREE.key(60));
			
			ret = AllocationMarker.freeRange(at, 3, 20);
			print(t,"free(3,20)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0),AllocationMarker.ALLOCATED.key(30),AllocationMarker.FREE.key(60));
			
			ret = AllocationMarker.freeRange(at, 0, 260);
			print(t,"free(0,60)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0));
			
			t.commit();
		} finally {
			if (t != null) t.close();
		}
	}
	
	@Test
	public void freeAdjacent() {
		BTreeBackingTransaction t = null;
		try {
			boolean ret;
			t = backing.open();
			final BTreeTransaction at = new TransactionBTree(t);
			
			print(t,"Start...");
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(0));
			
			ret = AllocationMarker.allocRange(at, 0, 100);
			print(t,"alloc(0,100)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(100));
			
			ret = AllocationMarker.freeRange(at, 10,10);
			print(t,"free(10,20)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(10),AllocationMarker.ALLOCATED.key(20),AllocationMarker.FREE.key(100));
			
			ret = AllocationMarker.freeRange(at, 30, 10);
			print(t,"free(30,10)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(10),AllocationMarker.ALLOCATED.key(20),AllocationMarker.FREE.key(30),AllocationMarker.ALLOCATED.key(40),AllocationMarker.FREE.key(100));
			
			ret = AllocationMarker.freeRange(at, 20, 10);
			print(t,"free(20,10)");
			assertTrue(ret);
			verify(t,AllocationMarker.ALLOCATED.key(0),AllocationMarker.FREE.key(10),AllocationMarker.ALLOCATED.key(40),AllocationMarker.FREE.key(100));
			
			t.commit();
		} finally {
			if (t != null) t.close();
		}
	}

	static void print(final BTreeBackingTransaction t, String msg) {
		print(BTree.Key.MIN_KEY, BTree.Key.MAX_KEY,t,msg);
	}
	
	static void print(BTree.Key from, BTree.Key to, final BTreeBackingTransaction t, String msg) {
		final BTreeBackingTransaction rt = new NonCommittingDelegateBackingTransaction( new ReadOnlyDelegateBackingTransaction(t) );
		BTree bt = new BackedBTree(new BTreeBacking() {
			@Override
			public void close() {
				
			}
			
			@Override
			public BTreeBackingTransaction open() {
				return rt;
			}
			
			@Override
			public ReadOnlyBTreeBackingTransaction openReadOnly() {
				return rt;
			}
		});
		System.out.printf("(%14s) -> ",msg);
		for (BTree.Key k : bt.open().range(from,to)) {
			assertNotNull(k);
			System.out.print(AllocationMarker.toString(k));
		}
		System.out.println();
	}
	
	static void verify(final BTreeBackingTransaction t, BTree.Key... markers) {
		final BTreeBackingTransaction rt = new NonCommittingDelegateBackingTransaction( new ReadOnlyDelegateBackingTransaction(t) );
		BTree bt = new BackedBTree(new BTreeBacking() {
			@Override
			public void close() {
				
			}
			
			@Override
			public BTreeBackingTransaction open() {
				return rt;
			}
			
			@Override
			public ReadOnlyBTreeBackingTransaction openReadOnly() {
				return rt;
			}
		});
		int i = 0;
		for (BTree.Key k : bt.open().range(BTree.Key.MIN_KEY, BTree.Key.MAX_KEY)) {
			assertTrue(i < markers.length);
			assertEquals(markers[i++],k);
		}
		assertEquals(markers.length,i);
	}
}
