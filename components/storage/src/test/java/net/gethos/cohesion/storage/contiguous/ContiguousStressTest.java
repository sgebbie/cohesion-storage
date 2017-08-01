/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.BTrees;
import net.gethos.cohesion.storage.ReadOnlyBTreeTransaction;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class ContiguousStressTest {

	@Before
	public void setUp() {
	}

	@Test
	public void fillVarious() {
		fillVarious(50,5000,2000,false);
	}

	@Test
	public void fillLots() {
		fillVarious(5,9000,11000,true);
		//		 fillVarious(500,9000,30000,true);
	}

	@Test
	public void fillWithExtranode() {
		BTree btree = BTrees.newInstance();
		int seed = 3;
		Random random = new Random(seed);
		fill(new ArrayList<BTree.Key>(),btree,4096,1000,random);
	}

	@Test
	public void fillOnlyIntranode() {
		BTree btree = BTrees.newInstance();
		int seed = 3;
		Random random = new Random(seed);
		fill(new ArrayList<BTree.Key>(),btree,1800,1000,random);
	}

	private void fillVarious(int runs, int maxDataSize, int numKeys, boolean progress) {
		int maxmax = 0;
		for(int i = 0; i < runs; i++) {
			AbstractWritableRootContiguousTransaction.PEEK_GUARD = 0;
			try {
				BTree bt = BTrees.newInstance(); //BTrees.newHeapInstance(); //
				fillRun(bt, i, maxDataSize, numKeys, progress, maxmax);
			} catch (Throwable t) {
				t.printStackTrace();
				throw new AssertionError(String.format("Error during test seed [%d] runs=%d, maxDataSize=%d, numKeys=%d",i,runs,maxDataSize,numKeys));
			} finally {
				if (AbstractWritableRootContiguousTransaction.PEEK_GUARD > maxmax) maxmax = AbstractWritableRootContiguousTransaction.PEEK_GUARD;
			}
		}
	}

	private void fillRun(BTree bt, int seed, int maxDataSize, int numKeys, boolean progress, int maxmax) {

		Random rnd = new Random(seed);
		if (progress) System.out.printf("[%d]",seed);
		Collection<BTree.Key> insertedKeys = new HashSet<BTree.Key>();
		fill(insertedKeys,bt,maxDataSize,numKeys,rnd);

		boolean shuffle = rnd.nextBoolean();

		List<BTree.Key> collectedKeys = new ArrayList<BTree.Key>();
		if (shuffle) Collections.shuffle(collectedKeys, rnd);
		ReadOnlyBTreeTransaction t = bt.openReadOnly();
		try {
			for(BTree.Key k : t.range(BTree.Key.MIN_KEY, BTree.Key.MAX_KEY)) {
				if (AllocationMarker.isMarker(k)) continue; // don't fuck with the allocation markers
				collectedKeys.add(k);
			}
		} finally {
			t.close();
		}

		if (progress) {
			System.out.printf(" inserted count=%s collected count=%d guard-maxmax=%d%n",insertedKeys.size(),collectedKeys.size(),maxmax);
		}

		// double check that all inserts are present
		for (BTree.Key k : insertedKeys) {
			ReadOnlyBTreeTransaction w = bt.openReadOnly();
			try {
				BTree.Reference r = w.search(k);
				assertNotNull(r);
			} finally {
				w.close();
			}
		}

		// delete the keys
		for (BTree.Key k : insertedKeys /*collectedKeys*/) {
			BTreeTransaction w = bt.open();
			try {
				BTree.Reference r = w.delete(k);
				assertNotNull(r);
				boolean ok = w.commit();
				assertTrue(String.format("Error removing key: %s%n",k),ok);
			} finally {
				w.close();
			}
		}

		// double check that all deletes are no longer present
		for (BTree.Key k : insertedKeys) {
			ReadOnlyBTreeTransaction w = bt.openReadOnly();
			try {
				BTree.Reference r = w.search(k);
				assertNull(r);
			} finally {
				w.close();
			}
		}

		Collection<BTree.Key> missing = new TreeSet<BTree.Key>(insertedKeys);
		missing.removeAll(collectedKeys);
		assertEquals(0,missing.size());
	}

	private static void fill(Collection<BTree.Key> keys, BTree btree, int max, int repeat, Random random) {
		for(int i = 0; i < repeat; i++) {
			//			System.out.printf("i=%d%n",i);
			long dsize = random.nextInt(max) + 1;
			BTree.Key k = createKey(random);
			keys.add(k);
			BTreeTransaction t = btree.open();
			try {
				//				if (i == 40) {
				//					System.out.printf("key=%s dsize=%d%n", k,dsize);
				//					((ReadOnlyTransactionBTree)t).print();
				//
				//					printAllocations();
				//				}
				BTree.Reference r = t.search(k);
				assertNull(r); // should actually just skip, as might occur stastically
				r = t.truncate(k, dsize);
				assertNotNull(r);
				boolean ok = t.commit();
				assertTrue(ok);
			} finally {
				t.close();
			}
		}
	}

	private static BTree.Key createKey(Random rand) {

		Long h = rand.nextLong();
		assert(h != null);
		Long m = rand.nextLong();
		assert(m != null);
		Integer l = rand.nextInt();
		assert(l != null);

		BTree.Key key = createKey(h);
		return key;
	}

	private static BTree.Key createKey(long h) {
		BTree.Key key = new BTree.Key();
		key.idx = 1;
		key.idHigh = h;
		key.idMiddle = 0;
		key.idLow = 0;
		key.type = 0;
		key.parameter = 0;
		return key;
	}

	//	private void printAllocations() {
	//
	//		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
	//		try {
	//			System.out.printf("Allocations: %n");
	//			for(BTree.Key ak : bt.range(AllocationMarker.MIN.key(0), AllocationMarker.MAX_ALLOCATION_KEY)) {
	//				System.out.printf("%d %s ", ak.idHigh, AllocationMarker.flag(ak));
	//			}
	//			System.out.printf(".%n");
	//
	//		} finally {
	//			bt.close();
	//		}
	//	}
}
