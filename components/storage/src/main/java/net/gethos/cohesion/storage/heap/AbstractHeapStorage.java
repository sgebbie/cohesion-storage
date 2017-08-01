/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public abstract class AbstractHeapStorage {
	
	private final int capacity;
	protected final Map<Long, HeapNode> backing;
	private final Map<Long, ByteBuffer> raw;
	
	/**
	 * This holds the depth of the leaf nodes, where the depth of the root == 0.
	 */
	public int depth;
	
	/**
	 * The offset of the current root node.
	 */
	public long root;
	
	public AbstractHeapStorage(int capacity) {
		this.depth = 0;
		this.root = 0;
		this.capacity = capacity;
		this.backing = new HashMap<Long, HeapNode>();
		this.raw = new HashMap<Long, ByteBuffer>();
		
		initialise();
	}
	
	private void initialise() {
		int depth = 1;
		long root = alloc(false);
		if (root < 0) throw new OutOfMemoryError("Unable to allocate a root for the BTree");
		recordRoot(depth,root);
	}
	
	public int depth() {
		return depth;
	}
	
	public long root() {
		return root;
	}
	
	/* Backing Operations */
	/**
	 * write the data back to the backing store
	 */
	public abstract void record(long offset, BTreeNode n);
	
	public abstract BTreeNode retrieve(long offset);
	
	/**
	 * Write the super blocks with the reference to the root node.
	 * <p>
	 * This should include the root offset, tree depth, node capacity parameter. 
	 */
	public void recordRoot(int depth, long root) {
		this.depth = depth;
		this.root = root;
	}

	public long alloc(boolean isLeaf) {
		return isLeaf
			? allocLeaf()
			: allocIndex();
	}
	
	private long allocIndex() {
		long l = findFreeNode();
		HeapIndexNode n = new HeapIndexNode(capacity);
		backing.put(l, n);
		return l;
	}
	
	private long allocLeaf() {
		long l = findFreeNode();
		HeapLeafNode n = new HeapLeafNode(capacity);
		backing.put(l, n);
		return l;
	}

	public long free(long offset) {
		// clashes between 'backing' and 'raw' are prevented by making one even and the other odd
		HeapNode n = backing.remove(offset);
		if (n == null) {
			ByteBuffer bb = raw.remove(offset);
			return bb == null ? -1 : bb.capacity();
		} else {
			return 1;
		}
	}
	
	public long size(long offset) {
		ByteBuffer bb = raw.get(offset);
		return bb == null ? -1 : bb.capacity();
	}
	
	/**
	 * Next even offset that is free.
	 */
	private long findFreeNode() {
		long l;
		// find a free "offset" by performing a brute force search
		for (l = 0; backing.containsKey(l); l += 2) if (l > Long.MAX_VALUE - 2) return BTreeBackingTransaction.ALLOC_FAILED;
		return l;
	}
	
	/**
	 * Next odd offset that is free.
	 */
	private long findFreeBuffer() {
		long l;
		// find a free "offset" by performing a brute force search
		for (l = 1; raw.containsKey(l); l += 2)  if (l > Long.MAX_VALUE - 2) return BTreeBackingTransaction.ALLOC_FAILED;
		return l;
	}

	public long alloc(long length) {
		if (length > Integer.MAX_VALUE) return -1;
		long l = findFreeBuffer();
		ByteBuffer bb = ByteBuffer.allocate((int)length);
		bb.order(StorageConstants.NETWORK_ORDER);
		raw.put(l, bb);
		return l;
	}
	
	public long read(long offset, long objectOffset, ByteBuffer buffer) {
		ByteBuffer n = raw.get(offset);
		if (n == null) return 0;
		if (objectOffset > n.capacity()) return 0;
		n.clear();
		n.position((int)objectOffset);
		int l = Math.min(n.remaining(), buffer.remaining());
		n.limit(n.position() + l);
		buffer.put(n);
		n.clear();
		return l;
	}
	
	public long write(long offset, long objectOffset, ByteBuffer buffer) {
		ByteBuffer n = raw.get(offset);
		if (n == null) return 0;
		if (objectOffset > n.capacity()) return 0;
		n.clear();
		n.position((int)objectOffset);
		int l = Math.min(n.remaining(), buffer.remaining());
		buffer.limit(l);
		n.put(buffer);
		n.clear();
		return l;
	}
	
}
