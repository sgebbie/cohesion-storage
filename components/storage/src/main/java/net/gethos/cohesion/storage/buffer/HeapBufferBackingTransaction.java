/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * 
 * <h1>Representing Allocations Tables as a Self Contained Tree Index</h1>
 * 
 * Ultimately the space allocation in the underlying storage will
 * be represented as an index tree (binary space partition) consisting
 * of alternating allocated and free offsets. When operating on the
 * tree we need to perform allocations, frees, node updates. However,
 * by representing the free/allocated space as an index within the tree
 * itself, the transaction mechanism can work with the storage to
 * translate all alloc/free/update into just node updates.
 * <p>
 * So, when the operation is committed we simply perform a large number
 * of node updates. Some of these will actually represent alloc or free
 * while others will represent data storage.
 * <p>
 * Rather self referential, but elegant (if it works ;) ).
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapBufferBackingTransaction implements BTreeBackingTransaction {

	private final HeapBufferStorage storage;
	private final NodeCapacities nodeCapacities;
	
	private boolean isOpen;
	
	protected HeapBufferBackingTransaction(HeapBufferStorage storage, NodeCapacities nodeCapacities) {
		this.storage = storage;
		this.nodeCapacities = nodeCapacities;
		this.isOpen = true;
	}
	
	@Override
	public int maxItemData() {
		return nodeCapacities.leafCapacity()/2;
	}
	
	@Override
	public boolean commit() {
		return true;
	}
	
	@Override
	public boolean isOpen() {
		return isOpen;
	}
	
	@Override
	public boolean close() {
		boolean wasOpen = isOpen;
		this.isOpen = false;
		return wasOpen;
	}
	
	@Override
	public BTreeNode retrieve(long offset) {
		return storage.retrieve(offset);
	}

	@Override
	public long alloc(boolean isLeaf) {
		return storage.alloc(isLeaf);
	}

	@Override
	public int depth() {
		return storage.depth();
	}

	@Override
	public long root() {
		return storage.root();
	}

	@Override
	public void recordRoot(int depth, long root) {
		storage.recordRoot(depth, root);
	}

	@Override
	public void record(long offset, BTreeNode n) {
		storage.record(offset, n);
	}

	@Override
	public long free(long offset) {
		return storage.free(offset);
	}

	@Override
	public long read(long offset, long objectOffset, ByteBuffer buffer) {
		return storage.read(offset,objectOffset,buffer);
	}

	@Override
	public long alloc(long length) {
		return storage.alloc(length);
	}

	@Override
	public long write(long offset, long objectOffset, ByteBuffer buffer) {
		return storage.write(offset,objectOffset,buffer);
	}

	@Override
	public long free(long offset, long length) {
		return storage.free(offset,length);
	}
	
}
