/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.utils;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.ReadOnlyBTreeTransaction;

/**
 * A persistent map data structure.
 * <p>
 * This stored its data by marshaling it into and out of a BTree.
 * The concurrency supported is dependent on the concurrency supported
 * by the underlying BTree.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BTreeMap<K,V> { // TODO btree map: extend AbstractMap and implement the complete Map<K,V> interface
	
	private static final int MAX_GUARD = 500000;
	private static short TYPE_SINGLETON = 0;

	public static interface Matcher<K,V> { // Selector
		public byte[] hash(K key);
		public boolean matches(K key, V value);
		public K key(V value);
	}
	
	public static interface Marshaller<V> {
		public V fromByteBuffer(ByteBuffer data);
		public ByteBuffer toByteBuffer(V value);
	}
	
	private final short idx;
	private final BTree btree;
	private final Matcher<K,V> matcher;
	private final Marshaller<V> marshaler;
	
	public BTreeMap(BTree btree, short idx, Matcher<K,V> selector, Marshaller<V> marshaller) {
		this.btree = btree;
		this.matcher = selector;
		this.marshaler = marshaller;
		this.idx = idx;
	}
	
	public V put(K key, V value) {
		
		int guard = 0;
		for(;;) {
			BTreeTransaction bt = null;
			try {
				bt = btree.open();
						
				if (key == null || value == null) throw new NullPointerException("BTreeMap does not allow null keys or values.");
				BTree.Key k = toKey(key);
				ByteBuffer o = marshaler.toByteBuffer(value);
				V previous = null;
				
				BTree.Reference r = bt.search(k);
				if (r != null) {
					ByteBuffer dprime = ByteBuffer.allocate((int)r.size);
					// dang, possible collision
					bt.fetch(r, 0, dprime);
					V oprime = marshaler.fromByteBuffer(dprime);
					if (matcher.matches(key, oprime)) {
						previous = oprime;
						// simply replace (checking if the data should be resized)
						if (o.remaining() != r.size) {
							r = bt.truncate(k, o.remaining());		
						}
					} else {
						throw new UnsupportedOperationException("oops collisions are not yet supported :(");
		//				return false;
					}
				} else {
					r = bt.truncate(k, o.remaining());
				}
		
				bt.store(r, 0, o);
				
				boolean success = bt.commit();
				if (success) return previous;
				
				// ah, commit failed, lets replay :(
				//   beware, this only works if the BTree provides proper atomicity, and currently this is not the case for extranode data
				
				// but first, a little paranoia
				if (guard++ > MAX_GUARD) throw new AssertionError("BTreeMap.put() is spinning.");
			
			} finally {
				if (bt != null) bt.close();
			}
		}

	}

	public V get(K key) {
		if (key == null) throw new NullPointerException("BTreeMap does not allow null keys or values.");
		
		ReadOnlyBTreeTransaction bt = null;
		try {
			bt = btree.openReadOnly();
			BTree.Key k = toKey(key);
			BTree.Reference r = bt.search(k);
			if (r != null) {
				ByteBuffer dprime = ByteBuffer.allocate((int)r.size);
				bt.fetch(r, 0, dprime);
				V oprime = marshaler.fromByteBuffer(dprime);
				if (matcher.matches(key, oprime)) {
					return oprime;
				}
			}
			return null;
		} finally {
			bt.close();
		}
	}

	private BTree.Key toKey(K key) {
		BTree.Key k = key(matcher.hash(key));
		return k;
	}

	private BTree.Key key(byte[] hash) {
		if(hash == null || hash.length != 20) throw new IllegalArgumentException("The hash must be a 20 byte array.");
		ByteBuffer b = ByteBuffer.wrap(hash);
		b.order(StorageConstants.NETWORK_ORDER);
		BTree.Key k = new BTree.Key();
		k.idx = idx;
		k.idHigh = b.getLong();
		k.idMiddle = b.getLong();
		k.idLow = b.getInt();
		k.type = TYPE_SINGLETON;
		return k;
	}
}
