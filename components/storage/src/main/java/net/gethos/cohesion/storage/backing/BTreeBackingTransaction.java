/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;

import java.nio.ByteBuffer;


/**
 * The transaction isolates the tree operations
 * from the cache and backing. This makes it possible
 * to aggregate changes together in bulk operations
 * and trap suitable points for copy-on-write.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface BTreeBackingTransaction extends ReadOnlyBTreeBackingTransaction {

	public static final long ALLOC_FAILED = Long.MIN_VALUE;

	/* Write Operations */

	/* Super Block */

	/**
	 * Write the super blocks with the reference to the root node. That is,
	 * this re-bases the tree at a new root.
	 * <p>
	 * This should include the root offset, tree depth, node capacity parameter.
	 */
	public void recordRoot(int depth, long root);


	/* Nodes */

	/**
	 * Allocate space for a new node in the backing store.
	 * <p>
	 * @return the offset for locating the node within the backing, or <code>ALLOC_FAILED</code> if the allocation failed.
	 */
	public long alloc(boolean isLeaf);

	/**
	 * Free space previously allocated to a node in the backing store.
	 * <p>
	 * The amount of space to free is inferred from the node capacity.
	 * 
	 * @return the amount of space freed, or -1;
	 */
	public long free(long offset);

	/**
	 * Write the data back to the backing store.
	 */
	public void record(long offset, BTreeNode n);


	/* Raw Regions */

	/**
	 * Allocate a new data region.
	 * 
	 * @param length
	 * @return offset of the newly allocated region, or <code>ALLOC_FAILED</code> if the allocation failed.
	 */
	public long alloc(long length); // TODO btree extranode: consider changing this to realloc(long offset, long oldLength, long newLength)

	/**
	 * Free space previously allocated in the backing store.
	 * 
	 * @param offset
	 * @param length
	 * @return the amount of space freed, or -1
	 */
	public long free(long offset, long length);

	/**
	 * Write directly to a data region.
	 * 
	 * @param offset
	 * @param objectOffset
	 * @param buffer
	 * @return number of bytes written.
	 */
	public long write(long offset, long objectOffset, ByteBuffer buffer);
}
