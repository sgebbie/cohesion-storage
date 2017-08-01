/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;

import net.gethos.cohesion.storage.BTree.Key;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface BTreeIndexNode extends BTreeNode {
	
	public static final long INVALID_OFFSET = Long.MIN_VALUE;

	/**
	 * Provision storage space for a new index item in the index node
	 * such that this item can be addressed by the given key.
	 * 
	 * @param key
	 * @return the index of the new item
	 */
	public int alloc(Key key);

	/**
	 * Write offset data into the right-hand-child offset storage.
	 * 
	 * @param nOffset
	 */
	boolean writeRight(long nOffset);

	/**
	 * Write offset data into an index node item.
	 * 
	 * @param idx
	 * @param data
	 */
	public boolean write(int idx, long data);
	
	/**
	 * Read the item and obtain the related offset.
	 * 
	 * @param idx
	 * @return offset related to this index, or <code>BTreeIndexNode.INVALID_OFFSET</code> if not offset is found
	 */
	public long offset(int idx);
	
}
