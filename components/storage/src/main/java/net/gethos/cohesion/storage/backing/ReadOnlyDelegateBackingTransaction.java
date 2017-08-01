/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;

import java.nio.ByteBuffer;


/**
 * This wraps a transaction enforcing read-only access.
 * <p>
 * Note, however, if the transaction that is wrapped is
 * is used directly to create modifications, then this
 * transaction can still cause those modifications to
 * be committed. 
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class ReadOnlyDelegateBackingTransaction implements BTreeBackingTransaction {

	private final BTreeBackingTransaction delegate;
	
	public ReadOnlyDelegateBackingTransaction(BTreeBackingTransaction delegate) {
		this.delegate = delegate;
	}
	
	@Override
	public int maxItemData() {
		return delegate.maxItemData();
	}
	
	@Override
	public boolean commit() {
		return delegate.commit();
	}

	@Override
	public boolean close() {
		return delegate.close();
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
	public long read(long offset, long objectOffset, ByteBuffer buffer) {
		return delegate.read(offset, objectOffset, buffer);
	}

	@Override
	public long alloc(long length) {
		throw new IllegalAccessError("This is a readonly transaction delegating to: " + delegate);
	}

	@Override
	public long write(long offset, long objectOffset, ByteBuffer buffer) {
		throw new IllegalAccessError("This is a readonly transaction delegating to: " + delegate);
	}
	

	@Override
	public void recordRoot(int depth, long root) {
		throw new IllegalAccessError("This is a readonly transaction delegating to: " + delegate);
	}

	@Override
	public void record(long offset, BTreeNode n) {
		throw new IllegalAccessError("This is a readonly transaction delegating to: " + delegate);
	}

	@Override
	public long alloc(boolean isLeaf) {
		throw new IllegalAccessError("This is a readonly transaction delegating to: " + delegate);
	}

	@Override
	public long free(long offset) {
		throw new IllegalAccessError("This is a readonly transaction delegating to: " + delegate);
	}

	@Override
	public long free(long offset, long length) {
		throw new IllegalAccessError("This is a readonly transaction delegating to: " + delegate);
	}

}
