/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;


/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapBacking implements BTreeBacking {
	
	private final HeapDirectStorage storage;
	
	public HeapBacking(int capacity) {
		this.storage = new HeapDirectStorage(capacity);
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public BTreeBackingTransaction open() {
		// no point in using an isolating transaction since
		// the backing nodes are modified directly.
		return new HeapPassThroughBackingTransaction(storage);
	}

	@Override
	public ReadOnlyBTreeBackingTransaction openReadOnly() {
		return new HeapPassThroughBackingTransaction(storage);
	}
}
