/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.Transaction;



/**
 * The transaction isolates the tree operations
 * from the cache and backing. This makes it possible
 * to aggregate changes together in bulk operations
 * and trap suitable points for copy-on-write.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface ReadOnlyBTreeBackingTransaction extends Transaction {
	
	/* Read Operations */
	
	/**
	 * This holds the depth of the leaf nodes, where the depth of the root == 0.
	 */
	public int depth();
	
	/**
	 * The offset of the current root node.
	 */
	public long root();

	/**
	 * Fetch node data from the backing store.
	 */
	public BTreeNode retrieve(long offset);
	
	/**
	 * The maximum size of an items associated inline data before the data
	 * needs to be stored outside of a node.
	 * 
	 * @return maximum size of item data.
	 */
	public int maxItemData();
	
	/**
	 * Read data directly from a region.
	 * 
	 * @param offset
	 * @param objectOffset
	 * @param buffer
	 * @return number of bytes read
	 */
	public long read(long offset, long objectOffset, ByteBuffer buffer);
}
