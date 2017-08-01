/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BTreeKeyTest {

	@Test
	public void maxKey() {
		String expect = "7fff:ffffffffffffffffffffffffffffffffffffffff:7fff:0000000000000000";
		String invalidKey = BTree.Key.MAX_KEY.toString();
//		System.out.println(invalidKey);
		assertEquals(expect, invalidKey);
	}
	
	@Test
	public void parseKey() {
		BTree.Key k = new BTree.Key();
		k.idx = 1234;
		k.idHigh = 10203040506L;
		k.idMiddle = 70603110506L;
		k.idLow = 9182746;
		k.type = 333;
		k.parameter = -1;
		String ktext = k.toString();
//		System.out.println(ktext);
		BTree.Key kp = BTree.Key.parseKey(ktext);
		assertEquals(k,kp);
		assertEquals(-1,k.parameter);
	}
	
	@Test
	public void unsignedKey() {
		int c = BTree.Key.MIN_KEY.compareTo(BTree.Key.MAX_KEY);
		assert(c < 0);
	}
	
}
