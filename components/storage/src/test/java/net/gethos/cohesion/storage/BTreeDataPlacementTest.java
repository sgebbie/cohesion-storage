/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.contiguous.AllocationMarker;
import net.gethos.cohesion.storage.contiguous.WinnowingContiguousByteBufferBacking;
import net.gethos.cohesion.storage.heap.HeapCloneBacking;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test the placement of the data within nodes,
 * or within the backing store, depending on the size.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
@RunWith(Parameterized.class)
public class BTreeDataPlacementTest extends BTreeTestBase {

	private static final int TEST_CAPACITY = 200;

	private BTree btree;

	private Random random;

	private final Class<? extends BTreeBacking> backingClass;

	public BTreeDataPlacementTest(Class<? extends BTreeBacking> backingClass) {
		this.backingClass = backingClass;
	}

	@Parameters
	public static Collection<Object[]> parameters() {
		ArrayList<Object[]> runs = new ArrayList<Object[]>();

		runs.add(new Object[]{HeapCloneBacking.class});
		runs.add(new Object[]{WinnowingContiguousByteBufferBacking.class});

		return runs;
	}

	@Override
	protected Random random() {
		return random;
	}

	@Before
	public void setUp() {
		this.random = new Random(123);
		btree = newBTreeInstance(backingClass, TEST_CAPACITY);
	}

	@Test
	public void storeSmall() {
		storeSingle(100);
	}

	@Test
	public void storeLarge() {
		storeSingle(10000);
	}

	@Test
	public void storeSmallParts() {
		storeParts(true, 0,50,25,100);
	}

	@Test
	public void storeLargeParts() {
		storeParts(true, 0,10000,5000,10000);
	}

	@Test
	public void storeSmallPartsKey() {
		storeParts(false, 0,50,25,100);
	}

	@Test
	public void storeLargePartsKey() {
		storeParts(false, 0,10000,5000,10000);
	}

	@Test
	public void storeSmallThenLargePartsKey() {
		storeParts(false, 0,50,25,100,75,15000);
	}

	@Test
	public void storeLargeThenSmallPartsKey() {
		storeParts(false,0,15000,25,500);
	}

	private static class Block {
		int offset;
		byte[] b;
	}

	/**
	 * Note, if useRef == true then one transaction is used for all operations,
	 * since references are not valid outside of the transaction scope.
	 * <p>
	 * However, if useRef == false, then each span is updated in its own transaction,
	 * but the reference is not verified.
	 * 
	 * @param useRef
	 * @param spans
	 */
	private void storeParts(boolean useRef, int... spans) {

		assertTrue(spans.length % 2 == 0);

		Block[] blocks = new Block[spans.length/2];
		// build a copy of the expected data following
		// the merging of the various placements.
		// The resultant length will be the length of
		// the last placement since we truncate each time.
		byte[] merged = new byte[spans[spans.length-1] + spans[spans.length-2]];
		for(int i = 0; i < spans.length; i+=2) {
			Block block = new Block();
			blocks[i/2] = block;
			block.offset = spans[i];
			block.b = random(spans[i+1]);
			if (block.offset > merged.length) continue;
			int l = Math.min(merged.length, block.b.length);
			System.arraycopy(block.b, 0, merged, block.offset, l);
		}

		BTree.Key key = createKey(55);

		BTreeTransaction bt = null;
		if (useRef) {
			bt = btree.open();
		}

		BTree.Reference rlast = null;
		for(int i = 0; i < spans.length; i+=2) {
			Block block = blocks[i/2];
			if (useRef && rlast != null) {
				if (!useRef) {
					bt = btree.open();
				}
				assert(bt != null);
				rlast = bt.truncate(rlast, block.offset + block.b.length);
				int r = bt.store(rlast, block.offset, ByteBuffer.wrap(block.b));
				assertEquals(r,block.b.length);
				if (!useRef) {
					bt.commit();
					bt.close();
				}
			} else {
				if (!useRef) {
					bt = btree.open();
				}
				assert(bt != null);
				rlast = bt.truncate(key, block.offset + block.b.length);
				System.out.printf("rlast=%s%n",rlast);
				int r = bt.store(rlast, block.offset, ByteBuffer.wrap(block.b));
				assertEquals(r,block.b.length);
				if (!useRef) {
					bt.commit();
					bt.close();
				}
			}

			// dump allocation
			ReadOnlyBTreeTransaction rbt = btree.openReadOnly();
			try {
				System.out.printf("[%d] %s%n",i,AllocationMarker.toString(rbt));
			} finally {
				rbt.close();
			}
		}

		if (useRef) {
			retrieve(key, rlast, merged, bt);
		} else {
			retrieve(key, null, merged);
		}

		if (useRef) {
			assert(bt != null);
			bt.commit();
			bt.close();
		}
	}


	private void storeSingle(int size) {
		byte[] data = random(size);
		BTree.Key key = createKey(55);

		BTreeTransaction bt = btree.open();
		BTree.Reference r = bt.store(key, 0, ByteBuffer.wrap(data));
		bt.commit();
		bt.close();
		assertNotNull(r);

		retrieve(key, r, data);
	}

	private void retrieve(BTree.Key key, BTree.Reference expectedReference, byte[] expectedData) {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		retrieve(key, expectedReference, expectedData, bt);
		bt.close();
	}

	private void retrieve(BTree.Key key, BTree.Reference expectedReference, byte[] expectedData,
			ReadOnlyBTreeTransaction bt) {
		BTree.Reference s = bt.search(key);
		if (expectedReference != null) assertEquals(expectedReference,s);
		assertEquals(expectedData.length, s.size);
		byte[] buf = new byte[(int)s.size];
		bt.fetch(s, 0, ByteBuffer.wrap(buf));
		assertTrue(Arrays.equals(expectedData, buf));
	}
}

