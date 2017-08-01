/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.buffer.BufferNode;
import net.gethos.cohesion.storage.buffer.BufferRegion;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.cache.RegionCache;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * Implements the basics of a writable transaction for
 * a contiguous store.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public abstract class AbstractWritableContiguousTransaction extends AbstractWritableRootContiguousTransaction {

	// For performance, it might be better to use sorted arrays below.
	// This would reduce the allocation overhead, but would require
	// verifying the performance characteristics. The map could be
	// implemented by two arrays, one sorted key array and a value
	// array with element movements shadowing the movements in the
	// sorted array. Clearly these arrays would be extended if needed.

	private final Map<Long, BufferRegion> modifiedNodes;

	public AbstractWritableContiguousTransaction(ContiguousStore store, RegionCache<BufferRegion> nodeCache, NodeCapacities nodeCapacities) {
		super(store, nodeCache, nodeCapacities);

		this.modifiedNodes = new HashMap<Long, BufferRegion>();
	}

	abstract long allocateStorage(long capacity);
	abstract long freeStorage(long offset, long length);

	@Override
	public boolean commit() {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");

		// 1. write back all modified nodes into the store
		// 2. write back update depth & root, if modified, by stamping into the header in the store

		// seal
		commit_sealModified(modifiedNodes);

		// writing
		commit_writeModified(modifiedNodes);

		// stamp in header if depth or root changed
		commit_writeRoot();

		// could consider zeroing out removed nodes

		return super.commit();
	}

	// -- node access

	/**
	 * Request allocation of space in the storage to hold a new node.
	 * Additionally allocate a memory based node as part of
	 * this allocation.
	 */
	@Override
	public long alloc(boolean isLeaf) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		// allocate space from the allocation tree,
		// if this is successful then allocate an in memory node

		int c = isLeaf ? nodeCapacities.leafCapacity() : nodeCapacities.innerCapacity();
		long offset = allocateStorage(c);
		if (offset == BTreeBackingTransaction.ALLOC_FAILED) return offset;
		BufferNode n = isLeaf ? BufferNode.allocateLeaf(c) : BufferNode.allocateIndex(c);
		modifiedNodes.put(offset,n);

		return offset;
	}

	/**
	 * First look in the modified set,
	 * otherwise go to "the source".
	 */
	@Override
	public BTreeNode retrieve(long offset) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");

		BufferRegion n = modifiedNodes.get(offset);
		if (n != null) return (BTreeNode)n;
		return super.retrieve(offset);
	}

	/**
	 * Simply stash the node on the modified set for the moment.
	 */
	@Override
	public void record(long offset, BTreeNode n) {

		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");

		modifiedNodes.put(offset, (BufferNode)n);
	}

	/**
	 * This marks the node as removed and deallocate its storage.
	 * The actual change will happen during the commit.
	 */
	@Override
	public long free(long offset) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");

		BufferRegion n = modifiedNodes.remove(offset);
		if (n == null) {
			// not modified, so retrieve so we can obtain the capcity
			n = (BufferNode)super.retrieve(offset);
		}

		// Strictly speaking, not only must we be able to find the node but we must have been accessed
		// earlier within this transaction otherwise the caller should not have known the offset.
		// However, we do not necessarily keep a long running cache of unmodified nodes.
		if (n == null) throw new IllegalStateException("The node can not be freed not be accessed.");

		return freeStorage(offset, n.buffer().capacity());
	}

	// -- raw access

	@Override
	public long free(long offset, long length) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		return freeStorage(offset, length);
	}

	@Override
	public long alloc(long length) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		long offset = allocateStorage(length);
		return offset;
	}

	@Override
	public long write(long offset, long objectOffset, ByteBuffer buffer) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		return store.write(offset+objectOffset, buffer); // Note, this does not provide sufficient atomicity and isolation
	}
}
