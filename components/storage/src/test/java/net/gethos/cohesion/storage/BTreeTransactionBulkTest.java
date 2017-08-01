/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.buffer.HeapBufferBacking;
import net.gethos.cohesion.storage.contiguous.SynchronousContinguousFileBacking;
import net.gethos.cohesion.storage.contiguous.WinnowingContiguousByteBufferBacking;
import net.gethos.cohesion.storage.contiguous.WinnowingContiguousFileBacking;
import net.gethos.cohesion.storage.heap.HeapBacking;
import net.gethos.cohesion.storage.heap.HeapCloneBacking;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
@RunWith(Parameterized.class)
public class BTreeTransactionBulkTest extends BTreeTestBase {

	private static final int TEST_CAPACITY_BIG = 200;

	private final Class<? extends BTreeBacking> backingClass;
	private final int oneAtATime;

	public BTreeTransactionBulkTest(Class<? extends BTreeBacking> backingClass, int ontAtATime) {
		this.backingClass = backingClass;
		this.oneAtATime = ontAtATime;
	}

	@Parameters
	public static Collection<Object[]> parameters() {
		ArrayList<Object[]> runs = new ArrayList<Object[]>();

		runs.add(new Object[] { HeapBacking.class, 10000 });
		runs.add(new Object[] { HeapCloneBacking.class, 10000 });
		runs.add(new Object[] { HeapBufferBacking.class, 10000 });
		runs.add(new Object[] { WinnowingContiguousByteBufferBacking.class, 10000 });
		runs.add(new Object[] { WinnowingContiguousFileBacking.class, 10000 });
		runs.add(new Object[] { SynchronousContinguousFileBacking.class, 1000 });

		return runs;
	}

	private Random random;

	@Override
	protected Random random() {
		return random;
	}

	@Before
	public void setUp() {
		this.random = new Random(1234);
	}

	/**
	 * Test operations that are batched via transactions so as to reduce
	 * the number of commits.
	 */
	@Test
	public void buildAndDeleteInBatches() {
		BTree btree = newBTreeInstance(backingClass, TEST_CAPACITY_BIG);
		buildAndDelete(btree,10000,200,false);
		int depth = btree.depth();
		assertEquals(1,depth);
//		System.out.printf("force-count=%d",net.gethos.cohesion.storage.store.RandomAccessContiguousStore.fcount);
	}

	@Test
	public void buildAndDeleteOneAtATime() {
		BTree btree = newBTreeInstance(backingClass, TEST_CAPACITY_BIG);
		buildAndDelete(btree, oneAtATime, 1, false);
		int depth = btree.depth();
		assertEquals(1, depth);
//		System.out.printf("force-count=%d", net.gethos.cohesion.storage.store.RandomAccessContiguousStore.fcount);
	}

	protected void buildAndDelete(BTree btree, int elements, int chunk, boolean deleteInSameOrder) {
		Counters c = new Counters();

		Random rnd = random();
		List<Long> ids = new ArrayList<Long>();
		for (int i = 0; i < elements; i++) ids.add((long)i);
		Collections.shuffle(ids, rnd);
		c.data += c.diff();

//		System.out.println("storing...");
		BTreeTransaction bt = null;
		count = 0;
		for (long id : ids) {
			if (count % chunk == 0 || bt == null) {
				c.data += c.diff();
				if (bt != null) bt.commit();
				bt = btree.open();
				c.commit += c.diff();
			}
//			if (count % 10000 == 0) System.out.printf(" >[%d] id=%d%n", count, id);
			storeFetchOne(bt, c, id);
			count++;
		}
		if (bt != null) {
			c.data += c.diff();
			bt.commit();
			bt = null;
			c.commit += c.diff();
		}

//		System.out.println("store order  = " + ids);

		if (!deleteInSameOrder) Collections.shuffle(ids,rnd);
		c.data += c.diff();

//		System.out.println("delete order = " + ids);

//		System.out.println("deleting...");
		count = 0;
		for (long id : ids) {
			if (count % chunk == 0 || bt == null) {
				c.data += c.diff();
				if (bt != null) bt.commit();
				bt = btree.open();
				c.commit += c.diff();
			}
//			System.out.printf(" <[%d] id=%d%n", count, id);
			deleteOne(bt, c, id);
			count++;
		}
		if (bt != null) {
			c.data += c.diff();
			bt.commit();
			bt = null;
			c.commit += c.diff();
		}

		printCounters(String.format("%s %40s [%5d|%4d] ",
				this.getClass().getSimpleName(), backingClass.getSimpleName(),
				elements, chunk), btree, elements, c);
	}

	private void deleteOne(BTreeTransaction bt, Counters c, long id) {

		// now delete from a node that won't become too small
		BTree.Key k = createKey(id);
		c.data += c.diff();
		BTree.Reference refSearch = bt.search(k);
		c.search += c.diff();
		assertNotNull("Unable to find during search: " + id, refSearch);
		BTree.Reference refDelete = bt.delete(k);
		c.delete += c.diff();
		assertNotNull("Unable to find during delete: " + id,refDelete);
		assertEquals(refSearch, refDelete);

		// search again and check that the item is no longer in the tree
		BTree.Reference refSearchAgain = bt.search(k);
		c.search += c.diff();
		assertNull(refSearchAgain);
	}

}

