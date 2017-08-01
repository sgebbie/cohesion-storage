/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;

import java.nio.ByteBuffer;


/**
 * This wraps a transaction and simply ignores attempts to commit. 
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class NonCommittingDelegateBackingTransaction implements BTreeBackingTransaction {

	private final BTreeBackingTransaction delegate;
	
	public NonCommittingDelegateBackingTransaction(BTreeBackingTransaction delegate) {
		this.delegate = delegate;
	}
	
	@Override
	public int maxItemData() {
		return delegate.maxItemData();
	}
	
	@Override
	public boolean commit() {
		return true;
	}

	@Override
	public boolean close() {
		// quietly ignore
		return true;
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	@Override
	public int depth() {
		return delegate.depth();
	}

	@Override
	public long root() {
		return delegate.root();
	}

	@Override
	public BTreeNode retrieve(long offset) {
		return delegate.retrieve(offset);
	}

	@Override
	public void recordRoot(int depth, long root) {
		delegate.recordRoot(depth, root);
	}

	@Override
	public void record(long offset, BTreeNode n) {
		delegate.record(offset, n);
	}

	@Override
	public long alloc(boolean isLeaf) {
		return delegate.alloc(isLeaf);
	}

	@Override
	public long free(long offset) {
		return delegate.free(offset);
	}

	@Override
	public long read(long offset, long objectOffset, ByteBuffer buffer) {
		return delegate.read(offset, objectOffset, buffer);
	}

	@Override
	public long alloc(long length) {
		return delegate.alloc(length);
	}

	@Override
	public long write(long offset, long objectOffset, ByteBuffer buffer) {
		return delegate.write(offset, objectOffset, buffer);
	}

	@Override
	public long free(long offset, long length) {
		return delegate.free(offset, length);
	}
	
	
}
