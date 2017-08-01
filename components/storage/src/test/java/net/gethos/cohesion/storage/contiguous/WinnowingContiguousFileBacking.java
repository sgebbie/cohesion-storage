/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import net.gethos.cohesion.storage.store.RandomAccessContiguousStore;

/**
 * Implements a BTree backing which uses a single backing region stored in a file
 * and additionally maintains its own allocation tree within the tree itself.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class WinnowingContiguousFileBacking extends WinnowingContiguousBacking {
	public WinnowingContiguousFileBacking(int x) {
		super(RandomAccessContiguousStore.createTemporaryStore(false), new RandomAccessNodeCapacities(), true, false, true);
	}
}
