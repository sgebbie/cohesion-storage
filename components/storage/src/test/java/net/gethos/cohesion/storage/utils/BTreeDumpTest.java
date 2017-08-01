/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.BTrees;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 * 
 */
public class BTreeDumpTest {

	private static final int MAX_DATA_SIZE = 12 * 1024;
	private static final int NUM_KEYS = 2000;

	@Test
	public void equals() {
		BTree a = BTrees.newHeapInstance();
		BTree b = BTrees.newHeapInstance();

		populateRandomTree(a, new Random(123), NUM_KEYS);
		populateRandomTree(b, new Random(123), NUM_KEYS);

		assertTrue(BTreeUtils.equals(a, b));
	}

	@Test
	public void dumpAndLoad() throws IOException {

		Timer timer = new Timer();

		BTree btreeBefore = BTrees.newHeapInstance();

		populateRandomTree(btreeBefore, new Random(123), NUM_KEYS);

		System.out.printf("d1=%d%n", timer.duration(TimeUnit.MILLISECONDS));

		BTree btreeAfter = BTrees.newContiguousInstance();

		// dump the btree data
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		WritableByteChannel cout = Channels.newChannel(bout);
		BTreeDump.dump(btreeBefore, cout, 100);
		cout.close();
		bout.close();

		System.out.printf("d2=%d%n", timer.duration(TimeUnit.MILLISECONDS));

		// reload the btree data
		byte[] dumpdata = bout.toByteArray();
		System.out.printf("dumpdata.length=%d%n", dumpdata.length);
		ByteArrayInputStream bin = new ByteArrayInputStream(dumpdata);
		ReadableByteChannel cin = Channels.newChannel(bin);
		BTreeDump.load(btreeAfter, cin, 100);

		System.out.printf("d3=%d%n", timer.duration(TimeUnit.MILLISECONDS));

		// compare the trees
		assertTrue(BTreeUtils.equals(btreeBefore, btreeAfter));

		System.out.printf("d4=%d%n", timer.duration(TimeUnit.MILLISECONDS));
	}

	private void populateRandomTree(BTree btree, Random rand, int numKeys) {
		for (int i = 0; i < numKeys; i++) {

			BTreeTransaction t = btree.open();
			try {
				BTree.Key k = new BTree.Key();
				k.idx = (short) (rand.nextInt(BTree.Key.MAX_KEY.idx - BTree.Key.MIN_DATA_KEY.idx) + BTree.Key.MIN_DATA_KEY.idx);
				k.idHigh = rand.nextLong();
				k.idMiddle = rand.nextLong();
				k.idLow = rand.nextInt();
				k.type = (short) rand.nextInt(Short.MAX_VALUE);

				int size = rand.nextInt(MAX_DATA_SIZE);
				byte[] data = new byte[size];
				rand.nextBytes(data);

				ByteBuffer d = ByteBuffer.wrap(data);

				t.store(k, 0, d);

				t.commit();
			} finally {
				t.close();
			}
		}
	}

	private class Timer {

		private long lastMarker;

		public Timer() {
			this.lastMarker = System.currentTimeMillis();
		}

		public long duration(TimeUnit units) {
			long now = System.currentTimeMillis();
			long d = now - lastMarker;
			lastMarker = now;
			return units.convert(d, TimeUnit.MILLISECONDS);
		}
	}
}
