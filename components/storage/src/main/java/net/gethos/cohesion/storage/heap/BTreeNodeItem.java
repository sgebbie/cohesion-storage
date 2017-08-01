/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.backing.BTreeLeafNode;

public abstract class BTreeNodeItem implements Cloneable {
	
	/**
	 * The key used to find this item
	 */
	public Key key;

	/**
	 * The offset of the item data within the node
	 */
	public int offset;
	
	/**
	 * The size of the item data within the node
	 */
	public int size;
	
	/**
	 * Used to interpret how the object data is stored:
	 *   raw in the node, chunk of data in backing store, extent list referencing multiple chunks.
	 * <p>
	 * If the data is not stored raw in the node, then the item data will contain the
	 * location of the referenced chunk or extent list. Additionally, it might contain
	 * a checksum of the data in that region.
	 */
	public byte flags;
	
	public BTreeNodeItem(BTree.Key key, int offset, int size) {
		this.key = key;
		this.offset = offset;
		this.size = size;
		this.flags = BTreeLeafNode.Flags.NONE.mask;
	}

	@Override
	public abstract String toString();

	@Override
	public BTreeNodeItem clone() {
		try {
			BTreeNodeItem x = (BTreeNodeItem)super.clone();
			x.key = x.key.clone();
			return x;
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException("Clone is supported",e);
		}
	}
}