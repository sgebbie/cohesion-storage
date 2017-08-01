/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.RuntimeIOException;

/**
 * Store a block of keys.
 * <p>
 * This can be used to build a key based multimap, relating one key to many.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BTreeKeySet {
	
	/* Layout:
	 *   magic
	 *   count
	 *   smallest key
	 *   ...
	 *   largest key
	 */
	
	private static final byte[] MAGIC = new byte[]{(byte)'B',(byte)'T',(byte)'K',(byte)'S'};
	
	private final SortedSet<BTree.Key> keys;
	
	public BTreeKeySet() {
		this.keys = new TreeSet<BTree.Key>();
	}
	
	public BTreeKeySet(ByteBuffer buffer) {
		this();
		fromBuffer(keys,buffer);
	}
	
	public ByteBuffer toBuffer() {
		return toBuffer(keys);
	}

	public boolean put(BTree.Key value) {
		return keys.add(value);
	}
	
	public boolean contains(BTree.Key value) {
		return keys.contains(value);
	}
	
	public boolean remove(BTree.Key value) {
		return keys.remove(value);
	}
	
	public void clear() {
		keys.clear();
	}
	
	public SortedSet<BTree.Key> values() {
		return keys;
	}
	
	public int size() {
		return keys.size();
	}
	
	public boolean isEmpty() {
		return size() == 0;
	}
	
	private static ByteBuffer toBuffer(Collection<BTree.Key> keys) {
		int required = MAGIC.length;
		required += StorageConstants.SIZEOF_INT;
		required += keys.size() * BTree.Key.SIZE;
		
		ByteBuffer buf = ByteBuffer.allocate(required);
		buf.order(StorageConstants.NETWORK_ORDER);
		buf.put(MAGIC);
		buf.putInt(keys.size());
		
		for(BTree.Key k : keys) k.write(buf);
		
		buf.flip();
		return buf;
	}
	
	private static void fromBuffer(Collection<BTree.Key> keys, ByteBuffer buffer) {
		// check minimum required length
		int required = MAGIC.length;
		required += StorageConstants.SIZEOF_INT;
		if (buffer.remaining() < required) throw new RuntimeIOException(String.format("Insufficient data in key set buffer. Remaining was %d, required at least %d.", buffer.remaining(), required));
		
		// read and check magic
		byte[] magic = new byte[MAGIC.length];
		buffer.order(StorageConstants.NETWORK_ORDER);
		buffer.get(magic);
		if (!Arrays.equals(MAGIC, magic)) throw new RuntimeIOException(String.format("Bad magic {%s} in key set. Expected {%s}.", Arrays.toString(magic), Arrays.toString(MAGIC)));
		
		// read and check count
		int count = buffer.getInt();
		if (count < 0) throw new RuntimeIOException(String.format("Bad key set length {%d}. Should be zero for strictly positive.", count));

		// read in the keys
		for(int i = 0; i < count; i++) {
			BTree.Key k = new BTree.Key(buffer);
			keys.add(k);
		}
	}
}
