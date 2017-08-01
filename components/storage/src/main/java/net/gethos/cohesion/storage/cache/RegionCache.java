/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.cache;


/**
 * A cache of objects corresponding to particular regions identified by a <code>long</code> offset.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public interface RegionCache<T> {

	/**
	 * Obtain a cached value, if it exists and has been retained.
	 * 
	 * @param offset
	 * @return non null value is cached
	 */
	public T get(long offset);
	
	/**
	 * Store a value in the cache for potential future retrieval.
	 * 
	 * @param offset
	 * @param n
	 * @return old value
	 */
	public T cache(long offset, T n);
	
	/**
	 * Expunge potential cache entry
	 * 
	 * @param offset
	 * @return true if there was a cache entry
	 */
	public boolean invalidate(long offset);

	/**
	 * Empty the cache.
	 */
	public void clear();

}
