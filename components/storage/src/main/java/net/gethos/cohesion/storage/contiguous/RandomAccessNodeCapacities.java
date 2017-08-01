/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.buffer.NodeCapacities;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class RandomAccessNodeCapacities implements NodeCapacities {
	
	@Override
	public int innerCapacity() {
		return StorageConstants.DEFAULT_NODE_CAPACITY;
	}

	@Override
	public int leafCapacity() {
		return StorageConstants.DEFAULT_NODE_CAPACITY;
	}

	@Override
	public int capacity(boolean isLeaf) {
		return StorageConstants.DEFAULT_NODE_CAPACITY;
	}
	
}
