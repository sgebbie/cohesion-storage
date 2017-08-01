/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import net.gethos.cohesion.storage.store.RandomAccessContiguousStore;

public class SynchronousContinguousFileBacking extends WinnowingContiguousBacking {
	public SynchronousContinguousFileBacking(int x) {
		super(RandomAccessContiguousStore.createTemporaryStore(true), new RandomAccessNodeCapacities(), true, true, true);
	}
}
