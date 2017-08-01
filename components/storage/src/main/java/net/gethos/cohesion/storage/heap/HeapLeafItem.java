/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import java.util.Arrays;

import net.gethos.cohesion.storage.BTree;

class HeapLeafItem extends BTreeNodeItem {

	public byte[] data;

	protected HeapLeafItem(BTree.Key key, int size) {
		super(key, 0, size);
		this.data = new byte[size];
	}
	
	protected void realloc(int size) {
		if (data.length != size) {
			byte[] old = data;
			data = new byte[size];
			System.arraycopy(old, 0, data, 0, old.length < data.length ? old.length : data.length);
			super.size = data.length;
		}
	}

	@Override
	public String toString() {
		return String.format("[k:%s d:%8x]",key,Arrays.hashCode(data));
	}
	
	@Override
	public HeapLeafItem clone() {
		HeapLeafItem x = (HeapLeafItem)super.clone();
		System.arraycopy(x.data, 0, this.data, 0, this.data.length);
		return x;
	}
}