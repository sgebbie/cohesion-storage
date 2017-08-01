/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.cache;


/**
 * A trivial cache.
 * <p>
 * It does not actually perform any caching.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class TrivialCache<T> implements RegionCache<T> {
	
	public TrivialCache() {
	}
	
	@Override
	public T get(long offset) {
		return null;
	}
	
	@Override
	public T cache(long offset, T n) {
		return null;
	}

	@Override
	public boolean invalidate(long offset) {
		return false;
	}

	@Override
	public void clear() {
	}
}
