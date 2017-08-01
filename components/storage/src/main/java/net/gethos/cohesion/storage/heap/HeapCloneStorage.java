/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import net.gethos.cohesion.storage.backing.BTreeNode;


/**
 * This backing stores nodes and data on the heap,
 * but all items are cloned before being handed out.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapCloneStorage extends AbstractHeapStorage {
	
	public HeapCloneStorage(int capacity) {
		super(capacity);
	}
	
	/* Backing Operations */
	
	/**
	 * write the data back to the backing store
	 */
	@Override
	public void record(long offset, BTreeNode n) {
		HeapNode existing = backing.put(offset, ((HeapNode)n).clone());
		assert(existing != null);
	}
	
	@Override
	public HeapNode retrieve(long offset) {
		return backing.get(offset).clone();
	}
}
