/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;

import net.gethos.cohesion.storage.BTree;

/**
 * Access tree nodes.
 * 
 * This should also include:
 * 	- generation
 *  - checksum
 *  
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface BTreeNode {
	
	/**
	 * The number of child nodes or objects referenced by this node.
	 * 
	 * @return number of children referenced
	 */
	public int children();
	
	/**
	 * Obtain the size of the data stored in the node.
	 * 
	 * @return the size of the data stored in the node
	 */
//	public int size();
	
	/**
	 * Obtain the maximum amount of data that can be stored in the node.
	 * <p>
	 * i.e. this is the maximum amount of space that could be free.
	 * 
	 * @return the maximum size
	 */
//	public int capacity();
	
	/**
	 * If the size is less than half of the capacity.
	 * 
	 * @return true if less than half full.
	 */
	public boolean isHalfEmpty();

	/**
	 * Check if the node has reached its full capacity.
	 * <p>
	 * i.e. check that no more items can be added to this node, even if the associated item data is of zero length.
	 * 
	 * @return true if full.
	 */
	public boolean isFull();
	
	/**
	 * Check if this index refers to the item holding the right most key for the node.
	 * <p>
	 * Note, this is not the "right child" of the index node since the right-child does
	 * not have an associated key.
	 * 
	 * @param idx
	 * @return true if this is the index of the item holding the right most key for the node
	 */
	public boolean isRightHandItem(int idx);
	
	/**
	 * Obtain the right most explicit key.
	 * 
	 * @return the largest key explicitly referenced by the node
	 */
	public BTree.Key rightHandKey();
	
	/**
	 * Change the key associated with the item at the given index.
	 * 
	 * @param idx
	 * @param key
	 * @return true if modified
	 */
	public boolean modify(int idx, BTree.Key key);
	
	/**
	 * Obtain the key associated with the item at the given index.
	 * 
	 * @param idx
	 * @return the key, or null if not a valid index.
	 */
	public BTree.Key key(int idx);
	
	/**
	 * Search for an item in the node.
	 * 
	 * @param key
	 * @return The index of the item, or (-(insertion)-1) if the item does not exit.
	 */
	public int find(BTree.Key key);
	
	/**
	 * Delete the item at the given index from the node.
	 * <p>
	 * Note, for index nodes <code>idx == size</code> indicates that the 'right child' should be cleared.
	 * 
	 * @param idx
	 * @return true if the item could be removed.
	 */
	public boolean delete(int idx);
	
	/**
	 * Check if we can compress 'this' node and the 'sibling' into one
	 * node.
	 * 
	 * @param sibling
	 * @return true if the contents of both nodes could fit into the sibling
	 */
	public boolean isCompressibleWith(BTreeNode sibling);
	
	public boolean balance(BTreeNode sibling, boolean requireEmpty);
	
}
