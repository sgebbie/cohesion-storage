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
 * This backing stores nodes and data on the heap,
 * but all items are cloned before being handed out.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapCloneBacking implements BTreeBacking {
	
	private final HeapCloneStorage storage;
	
	public HeapCloneBacking(int capacity) {
		this.storage = new HeapCloneStorage(capacity);
	}
	
	@Override
	public void close() {
		
	}

	@Override
	public BTreeBackingTransaction open() {
		return new HeapIsolatingBackingTransaction(storage);
	}

	@Override
	public ReadOnlyBTreeBackingTransaction openReadOnly() {
		return new HeapIsolatingBackingTransaction(storage);
	}
}
