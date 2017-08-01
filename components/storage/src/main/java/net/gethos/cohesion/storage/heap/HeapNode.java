/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.backing.BTreeNode;


public abstract class HeapNode implements BTreeNode, Cloneable {
	
	/**
	 * Either interpreted as the block size, or as the order (m).
	 */
	protected final int capacity;
	
	/**
	 * Calculated as <code>&lceil;capacity/2&rceil;</code>.
	 */
	protected final int half;
	
	protected int size;
	
	public HeapNode(int capacity) {
		this.capacity = capacity;
		this.half = (capacity/2) + (capacity%2); // divide by 2 and round up, i.e. Math.ceil(capacity/2.0);
		this.size = 0;
	}
	
	protected int items() {
		return size;
	}
	
	@Override
	public abstract boolean balance(BTreeNode sibling, boolean requireEmpty);
	
	@Override
	public boolean isRightHandItem(int idx) {
		return idx == size - 1;
	}
	
	@Override
	public BTree.Key rightHandKey() {
		return key(size-1);
	}

	@Override
	public boolean isHalfEmpty() {
		return size < half;
	}
	
	@Override
	public boolean isFull() {
		return size >= capacity;
	}
	
	@Override
	public HeapNode clone() {
		try {
			return (HeapNode)super.clone();
		} catch (CloneNotSupportedException e) {
			throw new RuntimeException("Clone is supported",e);
		}
	}

	/**
	 * @return number of children referenced
	 */
	@Override
	public abstract int children();
	
	/**
	 * Search for an item in the node.
	 * 
	 * @param key
	 * @return The index of the item, or (-(insertion)-1) if the item does not exit.
	 */
	@Override
	public abstract int find(BTree.Key key);
	
	/**
	 * Delete the item at the given index from the node.
	 * <p>
	 * Note, for index nodes <code>idx == size</code> indicates that the 'right child' should be cleared.
	 * 
	 * @param idx
	 * @return true if the item could be removed.
	 */
	@Override
	public abstract boolean delete(int idx);

	/**
	 * Fetch an item from the node.
	 * 
	 * @param idx
	 * @return the <code>idx<sup>th</sup></code> item
	 */
	public abstract BTreeNodeItem item(int idx);
	
	/**
	 * Store an item into the items array. The result leaves the items array stored according to the item key.
	 * 
	 * @param <T>
	 * @param items
	 * @param item
	 * @return the index at which the item was stored or (-(insertion)-1) if the node capacity has been reached as the item could not be stored.
	 */
	protected <T extends BTreeNodeItem> int storeItem(T[] items, T item) {
		
		// perform a linear search within the node to find the insertion point
		// (TODO consider using a binary search if the size is greater than 16 or so)
		int insertion = 0;
		for (insertion = 0; insertion < size; insertion++) {
			int c = items[insertion].key.compareTo(item.key);
			if (c == 0) {
				// if the key is equal, then simply replace the item
				items[insertion] = item;
				return insertion;
			}
			if (c < 0) continue;
			else break;
		}
		
		// if we are full then 
		if (size >= capacity) {
			return (-(insertion) - 1);
		}
		
		// at this point 'insertion' is the correct position for the new item,
		// either within the array or equal to 'size'.
		System.arraycopy(items, insertion, items, insertion+1, size - insertion);
		items[insertion] = item;
		size++;
		return insertion;
	}
	
	/**
	 * Search for an item in the node.
	 * 
	 * @param <T>
	 * @param items
	 * @param key
	 * @return The index of the item, or (-(insertion)-1) if the item does not exit.
	 */
	protected <T extends BTreeNodeItem> int findItem(T[] items, BTree.Key key) {
		
		// perform a linear search within the node to find the insertion point
		// (TODO consider using a binary search if the size is greater than 16 or so)
		int insertion = 0;
		for (insertion = 0; insertion < size; insertion++) {
			int c = items[insertion].key.compareTo(key);
			if (c == 0) {
				// if the key exists then return the position
				return insertion;
			}
			if (c > 0) break; // found the first key larger than the key being searched for
		}
		
		// the key does not exist, so return the position that it would be inserted into
		return (-(insertion) - 1);
	}
	
	/**
	 * Remove a single item from the node.
	 * 
	 * @param idx
	 */
	protected <T> boolean deleteItem(T[] items, int idx) {
		if (idx < 0 || idx >= size) return false;
		
		System.arraycopy(items, idx+1, items, idx, size-idx-1);
		size--;
		items[size] = null;
		
		return true;
	}
	
	/**
	 * Calculates the floor of <code>x</code> over 2.
	 * <p>
	 * Only for <code>x >= 0</code>.
	 * 
	 * @param x
	 * @return <code>&lfloor;x/2&rfloor;</code>
	 */
	protected static int halfFloor(int x) {
		if ((x & 0x1) == 0x1) {
			return x/2 + 1;
		} else {
			return x/2;
		}
	}
	
	/**
	 * Calculates the ceiling of <code>x</code> over 2.
 	 * <p>
	 * Only for <code>x >= 0</code>.
	 * @param x
	 * @return <code>&lceil;x/2&rceil;</code>
	 */
	protected static int halfCeil(int x) {
		return x/2;
	}
}
