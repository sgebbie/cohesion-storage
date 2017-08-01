/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

/**
 * Provides read-only access to a BTree.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface ReadOnlyBTree {
	
	/**
	 * Obtain a new read-only transaction for accessing the B-Tree
	 * 
	 * @return btree transaction
	 */
	public ReadOnlyBTreeTransaction openReadOnly();

	/**
	 * Close the tree and release resources.
	 */
	public void close();
}
