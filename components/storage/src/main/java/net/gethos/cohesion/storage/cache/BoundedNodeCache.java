/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.cache;

import net.gethos.cohesion.storage.buffer.BufferRegion;

/**
 * Simple LRU bounded node cache
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BoundedNodeCache implements RegionCache<BufferRegion> {
	
	private static final int DEFAULT_INITIAL_SIZE = 16;
	private static final int DEFAULT_MAX_SIZE = 2000;
		
	private final LRUCache<Long, BufferRegion> unmodifiedNodes;
	
	public BoundedNodeCache(int initialSize, int maxSize) {
		this.unmodifiedNodes = new LRUCache<Long, BufferRegion>(initialSize, maxSize);
	}
	
	public BoundedNodeCache() {
		this(DEFAULT_INITIAL_SIZE, DEFAULT_MAX_SIZE);
	}
	
	@Override
	public BufferRegion get(long offset) {
		return unmodifiedNodes.get(offset);
	}
	
	@Override
	public BufferRegion cache(long offset, BufferRegion n) {
		return unmodifiedNodes.put(offset, n);
	}

	@Override
	public boolean invalidate(long offset) {
		return unmodifiedNodes.remove(offset) == null ? false : true;
	}

	@Override
	public void clear() {
		unmodifiedNodes.clear();
	}
	
}
