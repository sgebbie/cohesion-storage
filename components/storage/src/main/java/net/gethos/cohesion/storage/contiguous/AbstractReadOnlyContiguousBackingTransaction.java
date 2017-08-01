/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;
import net.gethos.cohesion.storage.buffer.BufferLeafNode;
import net.gethos.cohesion.storage.buffer.BufferNode;
import net.gethos.cohesion.storage.buffer.BufferSuperNode;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * Provides read-only access to tree data held in a contiguous store.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public abstract class AbstractReadOnlyContiguousBackingTransaction implements ReadOnlyBTreeBackingTransaction {

	private static final int DEFAULT_NODE_CAPACITY = StorageConstants.DEFAULT_NODE_CAPACITY;
	private static final int DEFAULT_NODE_FETCH = DEFAULT_NODE_CAPACITY;
	private static final int MIN_NODE_FETCH = BufferNode.HEADER_SIZE;
	private static final int MAX_NODE_FETCH = 8*DEFAULT_NODE_FETCH;
	
	static final int NODE_SIZE = DEFAULT_NODE_CAPACITY;
	
	static final long HEADER_OFFSET = 0;
	
	// Ensure that we do not use more than 1/2 of the node
	// (i.e. HEADER_SIZE + 2*(MAX_ITEM_DATA+ITEM_ENTRY_SIZE) <= DEFAULT_NODE_CAPACITY)
	private static final int MAX_ITEM_DATA = ((DEFAULT_NODE_CAPACITY-BufferNode.HEADER_SIZE)/2) - BufferLeafNode.ITEM_ENTRY_SIZE;

	final ContiguousStore store;
	
	private boolean isOpen;
	private Integer depth;
	private Long root;
	
	private int nodeFetch;
	
	public AbstractReadOnlyContiguousBackingTransaction(ContiguousStore store) {
		this.store = store;
		this.depth = null;
		this.root = null;
		this.isOpen = true;
		this.nodeFetch = DEFAULT_NODE_FETCH;
	}
	
	@Override
	abstract public BTreeNode retrieve(long offset);
	
	@Override
	public int maxItemData() {
		return MAX_ITEM_DATA;
	}
	
	@Override
	public boolean commit() {
		close();
		return true;
	}

	@Override
	public boolean close() {
		boolean wasOpen = isOpen;
		isOpen = false;
		return wasOpen;
	}

	@Override
	public boolean isOpen() {
		return isOpen;
	}

	@Override
	public int depth() {
		if (!isOpen) throw new IllegalStateException("The transaction is no longer open");
		fetchSuper();
		return depth;
	}

	@Override
	public long root() {
		if (!isOpen) throw new IllegalStateException("The transaction is no longer open");
		fetchSuper();
		return root;
	}
	
	private void fetchSuper() {
		if (depth == null || root == null) {
			ByteBuffer header = ByteBuffer.allocate(BufferSuperNode.SUPER_NODE_SIZE);
			store.read(HEADER_OFFSET, header);
			BufferSuperNode sn = BufferSuperNode.wrap(header);
			depth = sn.depth();
			root = sn.root();
		}
	}
	
	@Override
	public long read(long offset, long objectOffset, ByteBuffer buffer) {
		return store.read(offset + objectOffset, buffer);
	}

	BufferNode fetch(long offset) {
		ByteBuffer n = ByteBuffer.allocate(nodeFetch);
		store.read(offset, n);
		n.rewind();
		int c = BufferNode.capacity(n);
		if (c < MIN_NODE_FETCH) return null;
		
		// don't make assumptions about the node size, but adjust the fetch size for the next read
		if (c != n.capacity()) {
			if (c < n.capacity()) {
				// down-size and copy
				if (c >= MIN_NODE_FETCH) nodeFetch = c;
				n.limit(c);
				ByteBuffer np = ByteBuffer.allocate(c);
				np.put(n);
				n = np;
			} else {
				// re-read with extra
				if (c <= MAX_NODE_FETCH) nodeFetch = c;
				n = ByteBuffer.allocate(c);
				store.read(offset, n);
			}
			
			n.rewind();
		}
		
		return BufferNode.wrap(n);
	}
	
}
