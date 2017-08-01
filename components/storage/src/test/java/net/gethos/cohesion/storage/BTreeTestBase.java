/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import net.gethos.cohesion.storage.backing.BTreeBacking;

import static org.junit.Assert.*;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public abstract class BTreeTestBase {

	public static int count;

	private final byte[] randomData;
	private final ByteBuffer randomBuffer;

	private final byte[] readData;
	private final ByteBuffer readBuffer;

	public BTreeTestBase() {
		this.randomData = new byte[100];
		this.randomBuffer = ByteBuffer.wrap(randomData);

		this.readData = new byte[100];
		this.readBuffer = ByteBuffer.wrap(readData);
	}

	protected abstract Random random();

	protected BTree newBTreeInstance(Class<? extends BTreeBacking> backingClass, int testCapacity) {
		BTreeBacking backing;
		try {
			backing = newBackingInstance(backingClass, testCapacity);
			BTree btree = new BackedBTree(backing);
			return btree;
		} catch (Throwable e) {
			throw new RuntimeException("failed to create BTree", e);
		}

	}

	protected BTreeBacking newBackingInstance(Class<? extends BTreeBacking> backingClass, int testCapacity) throws SecurityException, NoSuchMethodException, IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
		Constructor<? extends BTreeBacking> backingConstructor = backingClass.getConstructor(int.class);
		BTreeBacking backing = backingConstructor.newInstance(testCapacity);
		return backing;
	}

	protected void buildAndDelete(BTree btree, int elements, boolean deleteInSameOrder) {
		Counters c = new Counters();

		Random rnd = random();
		List<Long> ids = new ArrayList<Long>();
		for (int i = 0; i < elements; i++) ids.add((long)i);
		Collections.shuffle(ids, rnd);
		c.data += c.diff();

//		System.out.println("storing...");
//		count = 0;
		for (long id : ids) {
//			if (count % 10000 == 0) System.out.printf(" >[%d] id=%d%n", count, id);
			storeFetchOne(btree, c, id);
//			count++;
		}

//		System.out.println("store order  = " + ids);

		if (!deleteInSameOrder) Collections.shuffle(ids,rnd);
		c.data += c.diff();

//		System.out.println("delete order = " + ids);

//		System.out.println("deleting...");
//		count = 0;
		for (long id : ids) {
//			System.out.printf(" <[%d] id=%d%n", count, id);
			deleteOne(btree, c, id);
//			count++;
		}

		printCounters(btree, elements, c);
	}

	private void deleteOne(BTree btree, Counters c, long id) {

		// now delete from a node that won't become too small
		BTree.Key k = createKey(id);
		c.data += c.diff();
		BTreeTransaction bt = btree.open();
		BTree.Reference refSearch = bt.search(k);
		c.search += c.diff();
		assertNotNull("Unable to find during search: " + id, refSearch);
		BTree.Reference refDelete = bt.delete(k);
		bt.commit();
		c.delete += c.diff();
		assertNotNull("Unable to find during delete: " + id,refDelete);
		assertEquals(refSearch, refDelete);

		// search again and check that the item is no longer in the tree
		ReadOnlyBTreeTransaction btr = btree.openReadOnly();
		BTree.Reference refSearchAgain = btr.search(k);
		btr.close();
		c.search += c.diff();
		assertNull(refSearchAgain);

	}

	protected void storeWithSplit(BTree btree, int elements) {

		Counters c = new Counters();

		Random rnd = random();
		List<Long> ids = new ArrayList<Long>();
		for (int i = 0; i < elements; i++) ids.add((long)i);
		Collections.shuffle(ids, rnd);
		c.data += c.diff();

//		int count = 0;
		for (long id : ids) {
//			System.out.printf("storing [%d] id = %d%n", count, id);
//			if (count++ % 10000 == 0) System.out.printf(" %d%n", count);
//			if (count == 27369) {
//				System.out.println("Dang lets try to see what the problem is...");
//			}
			// dump backing
//			System.out.printf("---%n");
//			BTreeBacking backing = ((BackedBTree)btree).backing();
//			if (backing instanceof HeapBufferBacking) ((HeapBufferBacking) backing).dump(System.out);
//			try {
				storeFetchOne(btree, c, id);
//			} catch (Throwable t) {
//				System.err.printf("count=%d%n", count);
//				fail(t.getMessage());
//			}
//			count++;
		}

		printCounters(btree, elements, c);
	}

	protected void printCounters(BTree btree, int elements, Counters c) {
		printCounters("", btree, elements, c);
	}

	protected void printCounters(String prefix, BTree btree, int elements, Counters c) {

		System.out.print(prefix
				+ String.format("count=%d depth=%d", elements, btree.depth())
				+ String.format(" data=%dms(%04.2f/s) store=%dms(%04.2f/s) fetch=%dms(%04.2f/s) search=%dms(%04.2f/s)",

					TimeUnit.MILLISECONDS.convert(c.data, TimeUnit.NANOSECONDS),
					TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)*((double)elements/(double)c.data),

					TimeUnit.MILLISECONDS.convert(c.store, TimeUnit.NANOSECONDS),
					TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)*((double)elements/(double)c.store),

					TimeUnit.MILLISECONDS.convert(c.fetch, TimeUnit.NANOSECONDS),
					TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)*((double)elements/(double)c.fetch),

					TimeUnit.MILLISECONDS.convert(c.search, TimeUnit.NANOSECONDS),
					TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)*((double)elements/(double)c.search)

				)
				+ (c.delete > 0
					? String.format(" del=%dms(%04.2f/s)",
								TimeUnit.MILLISECONDS.convert(c.delete, TimeUnit.NANOSECONDS),
								c.delete == 0.0 ? -1 : TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)*((double)elements/(double)c.delete)
						)
					: "")
				+ (c.commit > 0
					? String.format(" cmt=%dms(%04.2f/s)",
								TimeUnit.MILLISECONDS.convert(c.commit, TimeUnit.NANOSECONDS),
								c.commit == 0.0 ? -1 : TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)*((double)elements/(double)c.commit)
						)
					: "")
				+ String.format(" all=%dms(%04.2f/s)%n",
					TimeUnit.MILLISECONDS.convert(c.store + c.fetch + c.search + c.delete + c.commit, TimeUnit.NANOSECONDS),
					TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS)*((double)elements/(double)(c.store + c.fetch + c.search + c.delete + c.commit))
				)
		);
	}

	protected void storeFetchOne(BTree btree, long id) {
		storeFetchOne(btree, new Counters(),id);
	}

	protected void storeFetchOne(BTree btree, Counters c, long id) {
		byte[] value = random(randomData);
		randomBuffer.clear();
		BTree.Key key = createKey(id);
		c.data += c.diff();

		BTreeTransaction bt = btree.open();
//		BTree.Reference ref = btree.store(key, 0, value, 0, value.length);
		BTree.Reference ref = bt.store(key, 0, randomBuffer);
		c.store += c.diff();

		assertNotNull(ref);
		assertEquals(value.length, ref.size);

//		((HeapBTree)btree).print();
//		System.out.println("====");

		Arrays.fill(readData, (byte)0);
		readBuffer.clear();
		byte[] f = readData;
		c.data += c.diff();
//		int l = btree.fetch(ref,0,f,0,f.length);
		int l = bt.fetch(ref,0,readBuffer); // The reference can be used within the scope of the transaction
//		int l = bt.fetch(key,0,readBuffer); // If the transaction is exited, then the key must be used
		c.fetch += c.diff();
		assertEquals(ref.size, l);
		assertNotNull(f);
		assertTrue(value.length == f.length);
		assertTrue(Arrays.equals(value, f));
		boolean ok = bt.commit();
		assertTrue(ok);

		// search again after transaction commit
		ReadOnlyBTreeTransaction btr = btree.openReadOnly();
		BTree.Reference r = btr.search(key);
		c.search += c.diff();
		assertNotNull(r);
		assertNotSame(ref,r); // Note, although ref and r will often be equal, we can not be sure since the commit may trigger further deletes and re-balancing
		btr.close();
	}

	protected void storeFetchOne(BTreeTransaction bt, Counters c, long id) {
		byte[] value = random(randomData);
		randomBuffer.clear();
		BTree.Key key = createKey(id);
		c.data += c.diff();

		BTree.Reference ref = bt.store(key, 0, randomBuffer);
		c.store += c.diff();

		assertNotNull(ref);
		assertEquals(value.length, ref.size);

		Arrays.fill(readData, (byte)0);
		readBuffer.clear();
		byte[] f = readData;
		c.data += c.diff();
		int l = bt.fetch(ref,0,readBuffer); // The reference can be used within the scope of the transaction
		c.fetch += c.diff();
		assertEquals(ref.size, l);
		assertNotNull(f);
		assertTrue(value.length == f.length);
		assertTrue(Arrays.equals(value, f));

		// search again after transaction commit
		BTree.Reference r = bt.search(key);
		c.search += c.diff();
		assertNotNull(r);
	}

	protected BTree.Key createKey(long id) {
		return BTreeTestUtils.createKey(id);
	}

	protected byte[] random(int length) {
		return BTreeTestUtils.random(random(), length);
	}

	protected byte[] random(byte[] buf) {
		return BTreeTestUtils.random(random(), buf);
	}
}
