/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.cache;

import java.util.HashMap;
import java.util.Map;

import net.gethos.cohesion.storage.buffer.BufferNode;

/**
 * A naive unbounded node cache
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class UnboundedNodeCache implements RegionCache<BufferNode> {
		
	private final Map<Long, BufferNode> unmodifiedNodes;
	
	public UnboundedNodeCache() {
		this.unmodifiedNodes = new HashMap<Long, BufferNode>();
	}
	
	@Override
	public BufferNode get(long offset) {
		return unmodifiedNodes.get(offset);
	}
	
	@Override
	public BufferNode cache(long offset, BufferNode n) {
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
