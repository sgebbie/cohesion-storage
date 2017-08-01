/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;

/**
 * Implements read-only B-Tree access.
 *  
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface ReadOnlyBTreeTransaction extends ReadOnlyBTreeKeyHandlingTransaction {
	
	/**
	 * Fetch object data associated with the reference.
	 *
	 * @param ref
	 * @param objectOffset
	 * @param buffer
	 * @return the number of bytes copied into the buffer.
	 */
	public int fetch(BTree.Reference ref, long objectOffset, ByteBuffer buffer);
	
	/**
	 * Fetch item stats.
	 *
	 * @param key
	 * @return stats for the node, or null if the item was not found. 
	 */
	public BTree.Stat stat(BTree.Key key);

	// -- convenience methods
	
	/**
	 * Fetch the data associated with the key.
	 * <p>
	 * Equivalent to <code>fetch(search(key),objectOffset,buffer,bufferOffset,length)</code>.
	 * 
	 * @param key
	 * @param objectOffset
	 * @param buffer
	 * @return number of bytes copied.
	 */
	public int fetch(BTree.Key key, long objectOffset, ByteBuffer buffer);

}
