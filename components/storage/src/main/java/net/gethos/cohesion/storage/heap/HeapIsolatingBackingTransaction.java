/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * A heap backed transaction that isolates changes from the
 * tree until a commit.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapIsolatingBackingTransaction implements BTreeBackingTransaction {
	
	private final AbstractHeapStorage storage;
	private final Map<Long,BTreeNode> modified;
	private final Map<Long,BTreeNode> unmodified;
	private final Deque<Long> removed;
	private final Deque<Long> allocated;
	
	private boolean isOpen;
	private boolean isCommited;
	private Integer modifiedDepth;
	private Long modifiedRoot;
	
	protected HeapIsolatingBackingTransaction(AbstractHeapStorage storage) {
		this.storage = storage;
		this.isOpen = true;
		this.isCommited = false;
		this.modified = new HashMap<Long, BTreeNode>();
		this.unmodified = new HashMap<Long, BTreeNode>();
		this.removed = new ArrayDeque<Long>();
		this.allocated = new ArrayDeque<Long>();
		this.modifiedDepth = null;
		this.modifiedRoot = null;
	}
	
	@Override
	public int maxItemData() {
		return Integer.MAX_VALUE;
	}
	
	@Override
	public boolean commit() {
		for (Map.Entry<Long, BTreeNode> x : modified.entrySet()) {
			storage.record(x.getKey(), x.getValue());	
		}
		// Note, nodes that were modified earlier in the transaction,
		//       could get removed later on. So, the remove must be
		//       enacted last.
		for (Long l : removed) {
			long r = storage.free(l);
			assert(r >= 0);
		}
		if (modifiedDepth != null && modifiedRoot != null) {
			storage.recordRoot(modifiedDepth, modifiedRoot);
		}
		isCommited = true;
		close();
		return true;
	}
	
	@Override
	public boolean isOpen() {
		return isOpen;
	}

	@Override
	public boolean close() {
		if (!isCommited) {
			for (long l : allocated) {
				storage.free(l);
			}
			allocated.clear();
		}
		
		boolean wasOpen = isOpen;
		this.isOpen = false;
		return wasOpen;
	}

	@Override
	public int depth() {
		return modifiedDepth == null ? storage.depth() : modifiedDepth;
	}

	@Override
	public long root() {
		return modifiedRoot == null ? storage.root() : modifiedRoot;
	}

	@Override
	public void recordRoot(int depth, long root) {
		modifiedDepth = depth;
		modifiedRoot = root;
	}

	@Override
	public void record(long offset, BTreeNode n) {
		modified.put(offset, n);
	}

	@Override
	public BTreeNode retrieve(long offset) {
		BTreeNode n = modified.get(offset);
		if (n == null) n = unmodified.get(offset);
		if (n == null) {
			n = storage.retrieve(offset);
			unmodified.put(offset, n);
		}
		return n;
	}

	@Override
	public long free(long offset) {
//		return removed.add(offset) ? 1 : -1;

		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		
		boolean ok = removed.add(offset);
		if (!ok) throw new IllegalStateException("The node has already been freed as part of this transaction.");
		
		BTreeNode n = modified.get(offset);
		if (n != null) {
			modified.remove(offset);	
		} else {
			n = unmodified.get(offset);
		}
		if (n == null) throw new IllegalStateException("The node can not be freed if it has not been accessed within this transaction.");
		
		return 1;
	}
	
	@Override
	public long free(long offset, long length) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		
		boolean ok = removed.add(offset);
		if (!ok) throw new IllegalStateException("The region has already been freed as part of this transaction.");
		long size = storage.size(offset);
		if (size < 0) throw new IllegalStateException("The region can not be freed as it does not exist.");
		assert(length == size);
		
		return size;
	}


	@Override
	public long alloc(boolean isLeaf) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		
		long l = storage.alloc(isLeaf);
		allocated.add(l);
		return l;
	}

	@Override
	public long alloc(long length) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		
		long l = storage.alloc(length);
		allocated.add(l); // stash for later in case we need to clean up
		return l;
	}
	
	@Override
	public long read(long offset, long objectOffset, ByteBuffer buffer) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		if (removed.contains(offset)) throw new IllegalStateException("The region has already been freed as part of this transaction.");
		
		return storage.read(offset, objectOffset, buffer);
	}

	@Override
	public long write(long offset, long objectOffset, ByteBuffer buffer) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		if (removed.contains(offset)) throw new IllegalStateException("The region has already been freed as part of this transaction.");
		
		return storage.write(offset,objectOffset,buffer);
	}

}
