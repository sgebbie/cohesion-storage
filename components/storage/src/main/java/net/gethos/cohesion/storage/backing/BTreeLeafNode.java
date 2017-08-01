/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.BTree;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface BTreeLeafNode extends BTreeNode {
	
	public static enum Flags {
		NONE((byte)0)
		, EXTERNAL((byte)(1<<0))
		, EXTENTS ((byte)(1<<1))
		;
		
		public final byte mask;
		
		Flags(byte mask) {
			this.mask = mask;
		}
	}
	
	/**
	 * Insert or resize an item indexed by the given key and making
	 * provision for storing exactly <code>length</code> bytes.
	 * 
	 * @param key
	 * @param length
	 * @return the index of the new item, or (-insertion-1) if there is insufficient space to (re)allocate the item.
	 */
	public int realloc(BTree.Key key, int length);
	
	/**
	 * Resize an item indexed by the given <code>idx</code> and making
	 * provision for storing exactly <code>length</code> bytes.
	 * <p>
	 * Existing data is to be copied or truncated as necessary.
	 * 
	 * @param idx
	 * @param length
	 * @return the index of the item, or (-insertion-1) if there is insufficient space to (re)allocate the item.
	 */
	public int realloc(int idx, int length);

	/**
	 * Obtain the amount of data storage allocated to the given item (within the node).
	 * 
	 * @param idx
	 * @return the size of the data allocated for the item
	 */
	public int size(int idx);

	/**
	 * Attempts to copy at most <code>buffer.remaining()</code> bytes from the data buffer into
	 * the indexed node item. However, if there is insufficient space provisioned then
	 * less data will be copied.
	 * 
	 * @param idx
	 * @param objectOffset
	 * @param buffer
	 * @return number of bytes copied.
	 */
	public int write(int idx, long objectOffset, ByteBuffer buffer);
	
	/**
	 * Attempts to copy at most <code>buffer.remaining()</code> bytes from the node item
	 * into the buffer. However, if there is insufficient data, then
	 * less bytes may be copied.
	 * 
	 * @param idx
	 * @param objectOffset
	 * @param buffer
	 * @return the number of bytes, possibly 0, copied into the buffer
	 */
	public int read(int idx, long objectOffset, ByteBuffer buffer);
	
	/**
	 * Balances data between 'this' node and the 'sibling' node, while:
	 * <ul>
	 *   <li>including 'nitemSize' space for a new item indexed by 'nitemKey' if it is not null</li>
	 *   <li>meeting the the minimum 'requiredFree' space in this node</li>
	 *   <li>attempting to have both nodes share the same amount of data</li>
	 * </ul>
	 * 
	 * If 'nitemKey' was null then only the contents of 'this' and 'sibling' will be accommodated.
	 * 
	 * @param sibling - not to balance with
	 * @param nitemKey - extra key to insert, null if no extra key
	 * @param nitemSize - extra data space to reserve while balancing
	 * @param requireEmpty - true if this node is required to be empty after balancing
	 * @return true if the constraints could be met.
	 */
	public boolean balance(BTreeNode sibling, final BTree.Key nitemKey, final int nitemSize, boolean requireEmpty);

	/**
	 * Obtain the flags associated with this item.
	 * 
	 * @param idx
	 * @return byte containing bit flags for this item
	 */
	public byte flags(int idx);

	/**
	 * Set the flags associated with this item.
	 * 
	 * @param idx
	 */
	public void flags(int idx, byte flags);
}
