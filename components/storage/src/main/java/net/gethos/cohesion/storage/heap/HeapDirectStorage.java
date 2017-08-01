/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import net.gethos.cohesion.storage.backing.BTreeNode;


/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapDirectStorage extends AbstractHeapStorage {
	
	public HeapDirectStorage(int capacity) {
		super(capacity);
	}
	
	/* Backing Operations */
	
	/**
	 * write the data back to the backing store
	 */
	@Override
	public void record(long offset, BTreeNode n) {
		HeapNode existing = backing.put(offset, (HeapNode)n);
		assert(existing != null);
	}
	
	@Override
	public BTreeNode retrieve(long offset) {
		return backing.get(offset);
	}
	
}
