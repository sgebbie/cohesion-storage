/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;


/**
 * The transaction isolates the tree operations
 * from the cache and backing. This makes it possible
 * to aggregate changes together in bulk operations
 * and trap suitable points for copy-on-write.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface Transaction {
	
	/* Transaction Operations */
	
	/**
	 * Closes the transaction, resulting in the changes being discarded
	 * if commit() has not been called.
	 * <p>
	 * Thus, close must always be called to release resources, but commit
	 * only needs to be called if there are updates that need to be stored.
	 */
	public boolean close();
	
	/**
	 * Check if the transaction has been committed, or is still open.
	 * 
	 * @return true if the transaction has not been committed
	 */
	public boolean isOpen();
	
	/**
	 * Write any changes back to the backing. Following this the transaction
	 * is closed.
	 */
	public boolean commit();

}
