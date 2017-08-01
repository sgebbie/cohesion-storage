/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import net.gethos.cohesion.storage.StorageConstants;

/**
 * A store for a tree that is held in a single
 * large ByteBuffer.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class ByteBufferNodeCapacities extends BufferScaling implements NodeCapacities {

	public ByteBufferNodeCapacities() {
		super(StorageConstants.DEFAULT_NODE_CAPACITY);
	}
	
	public ByteBufferNodeCapacities(int capacity) {
		super(capacity);
	}

	@Override
	public int innerCapacity() {
		return innerCapacity;
	}

	@Override
	public int leafCapacity() {
		return leafCapacity;
	}

	@Override
	public int capacity(boolean isLeaf) {
		return isLeaf ? leafCapacity : innerCapacity;
	}

}
