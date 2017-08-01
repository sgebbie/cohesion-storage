/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import net.gethos.cohesion.storage.utils.BTreeKeySet;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BTreeKeySetTest {

	private BTree btree;
	private Random rand;
	private BTree.Key main;
	
	@Before
	public void setUp() {
		rand = new Random(123);
		btree = BTrees.newInstance();
		main = new BTree.Key();
		main.idx = 123;
		main.idHigh = 1;
	}
	
	@Test
	public void storeRetrieve() {
		BTreeKeySet keys = new BTreeKeySet();
		
		// build corpus out of order
		List<BTree.Key> corpus = new ArrayList<BTree.Key>();
		for(int id = 0; id < 100; id++) {
			BTree.Key k = new BTree.Key();
			k.idLow = id;
			k.idHigh = id*id;
			k.parameter = 2*id;
			corpus.add(k);
		}
		Collections.shuffle(corpus,rand);
		
		// store corpus in key set
		for (BTree.Key k : corpus) {
			boolean ok = keys.put(k);
			assertTrue(ok);
		}
		
		// convert set to binary buffer
		ByteBuffer buf = keys.toBuffer();
		
		// store the set
		BTreeTransaction t = btree.open();
		try {
			t.store(main, 0, buf);
			t.commit();
		} finally {
			t.close();
		}
		
		// fetch the set
		BTreeKeySet again;
		ReadOnlyBTreeTransaction tr = btree.openReadOnly();
		try {
			BTree.Reference r = tr.search(main);
			assertNotNull(r);
			ByteBuffer d = ByteBuffer.allocate((int)r.size);
			tr.fetch(r, 0, d);
			d.flip();
			again = new BTreeKeySet(d);
		} finally {
			tr.close();
		}
		
		// check contents
		assertNotNull(again);
		assertEquals(corpus.size(), again.size());
		for (BTree.Key k : corpus) {
			assertTrue(again.contains(k));
		}
		List<BTree.Key> ordered = new ArrayList<BTree.Key>();
		for (BTree.Key k : again.values()) ordered.add(k);
		for(int i = 1; i < ordered.size(); i++) {
			BTree.Key l = ordered.get(i - 1);
			BTree.Key h = ordered.get(i);
			assertTrue(l.compareTo(h) < 0);
		}
	}
}
