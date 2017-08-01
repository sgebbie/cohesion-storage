/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.util.Iterator;

/**
 * Implements read-only B-Tree key access.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface ReadOnlyBTreeKeyHandlingTransaction extends Transaction {
	
	/**
	 * Find an item stored in the tree.
	 *
	 * @param key
	 * @return a reference to the item, or null if the key does not exist in the tree
	 */
	public BTree.Reference search(BTree.Key key);

	/**
	 * Find the key that would select the object
	 * referenced by this reference.
	 *
	 * @param ref
	 * @return key
	 */
	public BTree.Key key(BTree.Reference ref);
	
	/**
	 * Find a key that exists in the tree, and is the largest key in the tree
	 * that is not greater than the selection key.
	 *
	 * @param key
	 * @return the largest key in the tree that is not greater than the selection key, or null if there is no suitable key.
	 */
	public BTree.Key floor(BTree.Key key);

	/**
	 * Find a key that exists in the tree, and is the smallest key in the tree
	 * that is not less than the selection key.
	 *
	 * @param key
	 * @return the smallest key in the tree that is not less than selection key, or null if there is no suitable key.
	 */
	public BTree.Key ceiling(BTree.Key key);
	
	/**
	 * Iterate over the keys in the tree as selected by the range.
	 *
	 * @param fromKey
	 * @param toKey
	 * @return an iterator that will walk across the keys in the requested range <code>[ceiling(fromKey),floor(toKey)]</code>.
	 */
	public Iterable<BTree.Key> range(BTree.Key fromKey, BTree.Key toKey);
	
	/**
	 * Iterate over the keys in the tree as selected by the range.
	 *
	 * @param fromKey
	 * @param toKey
	 * @return an iterator that will walk across the keys in the requested range <code>[floor(fromKey),ceiling(toKey)]</code>.
	 */
	public Iterable<BTree.Key> rangeOuter(BTree.Key fromKey, BTree.Key toKey);
	
	/**
	 * Iterate over item references using the given key range. More specifically,
	 * the range is found by first finding the ceiling and floor of the from and to keys respectively.
	 *
	 * @param fromKey
	 * @param toKey
	 * @return an iterator that will walk across all references for keys in the range <code>[ceiling(fromKey),floor(toKey)]</code>.
	 */
	public Iterable<BTree.Reference> walk(BTree.Key fromKey, BTree.Key toKey);
	
	/**
	 * Release any transaction or resources associated with
	 * the iterator.
	 * 
	 * @param i
	 * @return true if the iterator needed to be released
	 */
	public boolean close(Iterator<?> i);

}
