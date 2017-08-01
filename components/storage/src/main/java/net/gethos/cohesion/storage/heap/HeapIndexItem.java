/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import net.gethos.cohesion.storage.BTree.Key;

class HeapIndexItem extends BTreeNodeItem {
	
	public long child;
	
	/**
	 * @param key
	 * @param offset
	 * @param size
	 */
	protected HeapIndexItem(Key key) {
		super(key, 0, 8);
	}
	
	@Override
	public String toString() {
		return String.format("[k:%s c:%8d]",key,child); 
	}
	
	@Override
	public HeapIndexItem clone() {
		return (HeapIndexItem)super.clone();
	}
}