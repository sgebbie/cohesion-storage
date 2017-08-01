/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import net.gethos.cohesion.storage.heap.HeapCloneBacking;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BTreeTruncationTest extends BTreeTestBase {
	
	private static final int TEST_CAPACITY = 200;

	private BTree btree;
	
	private Random random;
	
	@Override
	protected Random random() {
		return random;
	}
	
	@Before
	public void setUp() {
		this.random = new Random(123);
		btree = newBTreeInstance(HeapCloneBacking.class, TEST_CAPACITY);
	}
	
	@Test
	public void truncateSmall() {
		testTruncate(40, 20);
	}

	@Test
	public void truncateLarge() {
		testTruncate(40000, 20000);
	}
	
	private void testTruncate(int before, int after) {
		byte[] data = random(before);
		BTree.Key key = createKey(55);
		byte[] check = new byte[after];
		System.arraycopy(data, 0, check, 0, check.length);
		
		BTreeTransaction bt = btree.open();
		BTree.Reference r = bt.store(key, 0, ByteBuffer.wrap(data));
		bt.commit();
		bt.close();
		
		assertNotNull(r);
		ReadOnlyBTreeTransaction btr = btree.openReadOnly();
		r = btr.search(key);
		btr.close();
		assertNotNull(r);
		assertEquals(before, r.size);
		
		bt = btree.open();
		r = bt.truncate(key, after);
		bt.commit();
		bt.close();
		assertEquals(after, r.size);
		
		btr = btree.openReadOnly();
		r = btr.search(key);
		assertNotNull(r);
		assertEquals(after, r.size);
		
		byte[] fetched = new byte[after];
		btr.fetch(r, 0, ByteBuffer.wrap(fetched));
		
		btr.close();
		
		assertTrue(Arrays.equals(check, fetched));
	}

}
