/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.backing;



/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface BTreeBacking {
	
	/**
	 * Close the backing and release resources.
	 */
	public void close();
	
	/**
	 * Create a new transaction for interacting with
	 * the backing store.
	 * 
	 * @return a backing transaction
	 */
	public BTreeBackingTransaction open();
	
	/**
	 * Create a new read-only transaction for accessing the backing store.
	 * 
	 * @return a backing transaction
	 */
	public ReadOnlyBTreeBackingTransaction openReadOnly();
}
