/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTestUtils;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.BackedBTree;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.store.ContiguousStore;
import net.gethos.cohesion.storage.store.RandomAccessContiguousStore;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class ContiguousTreeTest {

	private Random rand;
	private ContiguousStore store;
	private NodeCapacities nodeCapacities;
	private BTree bt;

	@Before
	public void setUp() throws IOException {
		rand = new Random(123);

		File tmpFile = File.createTempFile("ContiguousTreeTest_", ".store");
		tmpFile.deleteOnExit();

		// bootstrap
		store = new RandomAccessContiguousStore(tmpFile, false, 4096*4);
		nodeCapacities = new RandomAccessNodeCapacities();
		// store = new ByteBufferContiguousStore(4096*4);

		// now create a tree
		WinnowingContiguousBacking backing = new WinnowingContiguousBacking(store, nodeCapacities, true, true, true);
		bt = new BackedBTree(backing);
	}

	@After
	public void tearDown() {
		if (store instanceof RandomAccessContiguousStore) {
			((RandomAccessContiguousStore) store).close();
		}
	}

	/**
	 * Test making updates to an allocation tree stored in
	 * a contiguous backed tree.
	 */
	@Test
	public void contiguousUpdate() {

		byte[] rdata = BTreeTestUtils.random(120);
		ByteBuffer rdatab = ByteBuffer.wrap(rdata);

		BTree.Key k = BTreeTestUtils.createKey(1000);
		BTreeTransaction btt = bt.open();
		BTree.Reference r = btt.truncate(k, rdata.length);
		btt.store(r, 0, rdatab);
		btt.commit();
		assertNotNull(r);
		print(bt,"A");

		List<Integer> inserts = new ArrayList<Integer>();
		for (int i = 0; i < 200; i++) inserts.add(i);
		Collections.shuffle(inserts, rand);

		for (int i : inserts) {
			btt = bt.open();
			k = BTreeTestUtils.createKey(i);
			r = btt.truncate(k, rdata.length);
			rdatab.clear();
			btt.store(r, 0, rdatab);
			btt.commit();
			assertNotNull(r);
		}

		print(bt,"B");

		Collections.shuffle(inserts, rand);

		//		int c = 0;
		for (int i : inserts) {
			//			System.out.printf("deleting[%d]: %d%n", c++, i);
			btt = bt.open();
			k = BTreeTestUtils.createKey(i);
			r = btt.delete(k);
			btt.commit();
			assertNotNull(r);
		}

		print(bt,"C");

		k = BTreeTestUtils.createKey(1000);
		btt = bt.open();
		btt.delete(k);
		btt.commit();
		assertNotNull(r);

		print(bt,"D");
	}

	/**
	 * Test reinserting at an existing key.
	 */
	@Test
	public void reinsert() {

		byte[] rdata = BTreeTestUtils.random(120);
		ByteBuffer rdatab = ByteBuffer.wrap(rdata);

		BTreeTransaction btt = bt.open();
		BTree.Key k = BTreeTestUtils.createKey(1000);
		BTree.Reference r = btt.truncate(k, rdata.length);
		btt.store(r, 0, rdatab);
		btt.commit();
		assertNotNull(r);
		print(bt,"reinsert A");

		// read and check
		btt = bt.open();
		byte[] rout = new byte[rdata.length];
		btt.fetch(r, 0, ByteBuffer.wrap(rout));
		assertTrue(Arrays.equals(rdata, rout));
		btt.close();

		// and again...
		btt = bt.open();
		byte[] rextra = BTreeTestUtils.random(40);
		r = btt.truncate(k, rdata.length + rextra.length);
		btt.store(r, rdata.length, ByteBuffer.wrap(rextra));
		btt.commit();
		assertNotNull(r);
		print(bt,"reinsert B");

		// read and check again
		btt = bt.open();
		byte[] routagain = new byte[rdata.length + rextra.length];
		btt.fetch(r, 0, ByteBuffer.wrap(routagain));
		btt.close();
		byte[] rd = new byte[rdata.length];
		byte[] re = new byte[rextra.length];
		System.arraycopy(routagain, 0, rd, 0, rd.length);
		System.arraycopy(routagain, rdata.length, re, 0, re.length);
		assertTrue(Arrays.equals(rdata, rd));
		assertTrue(Arrays.equals(rextra, re));
	}

	private void print(final BTree bt, String msg) {
		print(BTree.Key.MIN_KEY, BTree.Key.MAX_KEY,bt,msg);
	}

	private void print(BTree.Key from, BTree.Key to, final BTree bt, String msg) {
		System.out.printf("(%14s) -> ",msg);
		for (BTree.Key k : bt.open().range(from,to)) {
			assertNotNull(k);
			if (AllocationMarker.isMarker(k)) System.out.print(AllocationMarker.toString(k));
			else System.out.printf("%n[%s]",k);
		}
		System.out.println();
	}
}
