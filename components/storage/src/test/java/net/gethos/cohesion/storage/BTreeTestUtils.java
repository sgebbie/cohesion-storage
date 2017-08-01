/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.util.Random;

/**
 * Various utils to help testing.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 */
public class BTreeTestUtils {
	
	public static BTree.Key createKey(long id) {
		BTree.Key key = new BTree.Key();
		key.idx = 1;
		key.idHigh = id;
		key.type = 0;
		return key;
	}
	
	public static byte[] random(Random rand, int length) {
		byte[] b = new byte[length];
		rand.nextBytes(b);
		return b;
	}
	
	public static byte[] random(Random rand, byte[] b) {
		rand.nextBytes(b);
		return b;
	}
	
	public static byte[] random(int length) {
		return random(new Random(), length);
	}
	
	public static byte[] random(byte[] b) {
		return random(new Random(), b);
	}
}
