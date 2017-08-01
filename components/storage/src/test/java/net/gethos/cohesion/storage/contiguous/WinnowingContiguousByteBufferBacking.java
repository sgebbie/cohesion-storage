/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import net.gethos.cohesion.storage.buffer.ByteBufferNodeCapacities;
import net.gethos.cohesion.storage.store.ByteBufferContiguousStore;

/**
 * Implements a BTree backing which uses a single backing region stored in a byte buffer
 * and additionally maintains its own allocation tree within the tree itself.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class WinnowingContiguousByteBufferBacking extends WinnowingContiguousBacking {
	public WinnowingContiguousByteBufferBacking(int x) {
		super(new ByteBufferContiguousStore(4096*4), new ByteBufferNodeCapacities(),true,false,true);
	}
}
