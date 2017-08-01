/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapPassThroughBackingTransaction implements BTreeBackingTransaction {
	
	private final AbstractHeapStorage storage;
	
	private boolean isOpen;
	
	protected HeapPassThroughBackingTransaction(AbstractHeapStorage storage) {
		this.storage = storage;
		this.isOpen = true;
	}
	
	@Override
	public int maxItemData() {
		return Integer.MAX_VALUE;
	}
	
	@Override
	public boolean commit() {
		close();
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
		storage.record(offset,n);
	}

	@Override
	public BTreeNode retrieve(long offset) {
		return storage.retrieve(offset);
	}

	@Override
	public long free(long offset) {
		return storage.free(offset);
	}

	@Override
	public long alloc(boolean isLeaf) {
		return storage.alloc(isLeaf);
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
		return storage.free(offset);
	}

}
