/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.buffer.HeapBufferBacking;
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

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
@RunWith(Parameterized.class)
public class BTreeBulkTest extends BTreeTestBase {

	private static final int CORPUS_MULTIPLIER = 100;

	private static final int TEST_CAPACITY_SMALL = 5;
	private static final int TEST_CAPACITY_MEDIUM = 20;
	private static final int TEST_CAPACITY_BIG = 200;

	private final Class<? extends BTreeBacking> backingClass;

	public BTreeBulkTest(Class<? extends BTreeBacking> backingClass, String name) {
		this.backingClass = backingClass;
	}

	@Parameters(name = "{index}: {1}")
	public static Collection<Object[]> parameters() {
		ArrayList<Object[]> runs = new ArrayList<Object[]>();

		runs.add(new Object[]{HeapBacking.class, HeapBacking.class.getSimpleName()});
		runs.add(new Object[]{HeapCloneBacking.class, HeapCloneBacking.class.getSimpleName()});
		runs.add(new Object[]{HeapBufferBacking.class, HeapBufferBacking.class.getSimpleName()});
		runs.add(new Object[]{WinnowingContiguousByteBufferBacking.class, WinnowingContiguousByteBufferBacking.class.getSimpleName()});
		runs.add(new Object[]{WinnowingContiguousFileBacking.class, WinnowingContiguousFileBacking.class.getSimpleName()});

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

	@Test
	public void largeWithSmallCapacity() {
		BTree btree = newBTreeInstance(backingClass, TEST_CAPACITY_SMALL);
		storeWithSplit(btree, 80 * CORPUS_MULTIPLIER);
	}

	@Test
	public void largeWithMediumCapacity() {
		BTree btree = newBTreeInstance(backingClass, TEST_CAPACITY_MEDIUM);
		storeWithSplit(btree, 500 * CORPUS_MULTIPLIER);
	}

	@Test
	public void largeWithBigCapacity() {
		BTree btree = newBTreeInstance(backingClass, TEST_CAPACITY_BIG);
		storeWithSplit(btree, 1000 * CORPUS_MULTIPLIER);
	}

	@Test
	public void largeBuildAndDeleteBigCapacity() {
		BTree btree = newBTreeInstance(backingClass, TEST_CAPACITY_BIG);
		buildAndDelete(btree, 500 * CORPUS_MULTIPLIER,false);
		int depth = btree.depth();
		assertEquals(1,depth);
	}

	/**
	 * Here the test items do to pack into the nodes exactly,
	 * so there is an amount of free space left over that
	 * is not enough hold a new item but is non zero.
	 * <p>
	 * This requires careful handling when splitting and balancing
	 * the nodes, as the balance operation must ensure that the
	 * correct node (from the split) has enough space to hold the
	 * new item.
	 */
	@Test
	public void balanceWithFreeRemainder() {
		BTree btree = newBTreeInstance(backingClass, 4096);
		storeWithSplit(btree, 30);
	}

	/**
	 * Main used to make it easier to launch for profiling with VisualVM.
	 */
	public static void main(String[] args) {
		try { Thread.sleep(10000); } catch (InterruptedException e) {e.printStackTrace();}
		BTreeBulkTest test = new BTreeBulkTest(WinnowingContiguousFileBacking.class,
				WinnowingContiguousFileBacking.class.getSimpleName());
//		BTreeBulkTest test = new BTreeBulkTest(ContiguousFileBacking.class);
		try {
			test.setUp();
			test.largeWithSmallCapacity();
		} finally {
			// test.tearDown();
		}
	}
}

