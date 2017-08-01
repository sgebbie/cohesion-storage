/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.io.File;
import java.util.Map;

import net.gethos.cohesion.storage.buffer.BufferRegion;
import net.gethos.cohesion.storage.buffer.ByteBufferNodeCapacities;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.contiguous.Integrity;
import net.gethos.cohesion.storage.contiguous.RandomAccessNodeCapacities;
import net.gethos.cohesion.storage.contiguous.WinnowingContiguousBacking;
import net.gethos.cohesion.storage.contiguous.WinnowingIntegrity;
import net.gethos.cohesion.storage.heap.HeapBacking;
import net.gethos.cohesion.storage.store.ByteBufferContiguousStore;
import net.gethos.cohesion.storage.store.ContiguousStore;
import net.gethos.cohesion.storage.store.RandomAccessContiguousStore;

/**
 * Factory for BTrees.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BTrees {

	private static final int MIN_CAPACITY = 4096*4;

	/**
	 * A new BTree stored in the heap.
	 * 
	 * @return heap backed B-Tree
	 */
	public static BTree newInstance() {
		return newContiguousInstance();
	}

	// -- contiguous heap storage

	/**
	 * A new BTree stored in contiguous space in the heap.
	 * 
	 * @return heap backed B-Tree
	 */
	public static BTree newContiguousInstance() {
		return newContiguousInstance(MIN_CAPACITY);
	}

	/**
	 * A new BTree stored in contiguous space in the heap,
	 * and initialised with provisioning for <code>capacity</code> space.
	 * 
	 * @param capacity
	 * @return heap backed B-Tree
	 */
	public static BTree newContiguousInstance(int capacity) {
		ContiguousStore contiguousStore = new ByteBufferContiguousStore(capacity);
		NodeCapacities nodeCapacities = new ByteBufferNodeCapacities();
		WinnowingContiguousBacking backing = new WinnowingContiguousBacking(contiguousStore, nodeCapacities, true, false, true);
		BackedBTree btree = new BackedBTree(backing);
		return btree;
	}

	// -- node heap storage

	/**
	 * A new BTree stored as node objects in the heap.
	 * 
	 * @return heap backed B-Tree.
	 */
	public static BTree newHeapInstance() {
		HeapBacking backing = new HeapBacking(128);
		BackedBTree btree = new BackedBTree(backing);
		return btree;
	}

	// -- file storage

	/**
	 * A new BTree stored in a file without synchronous file updates.
	 * <p>
	 * Convenience method, equivalent to: <code>BTrees.newInstance(storage,false)</code>.
	 * 
	 * @return file backed B-Tree
	 */
	public static BTree newInstance(File storage) {
		return newInstance(storage,false);
	}

	/**
	 * A new BTree stored in a file.
	 * 
	 * @param sync - true to enable synchronous file access and integrity checking
	 * @return file backed B-Tree
	 */
	public static BTree newInstance(File storage, boolean sync) {
		boolean newStore = !storage.exists();
		ContiguousStore contiguousStore = new RandomAccessContiguousStore(storage, sync, MIN_CAPACITY);
		return newInstance(contiguousStore, newStore, sync, true);
	}

	// -- contiguous storage

	/**
	 * A new BTree stored in a contiguous store
	 * <p>
	 * If this is not a new tree (<code>bootstrap == false</code>) and integrity checking is
	 * enabled, this this will first perform crash recovery and possibly a full tree check
	 * if recovery was required.
	 * 
	 * @param bootstrap - true if the store should be considered empty and bootstrapped with a new tree
	 * @return contiguous backed B-Tree
	 */
	public static BTree newInstance(ContiguousStore store, boolean bootstrap, boolean integrity) {
		return newInstance(store,bootstrap,integrity,false);
	}

	private static BTree newInstance(ContiguousStore store, boolean bootstrap, boolean integrity, boolean closeStore) {
		if (integrity && !bootstrap) {
			// automatic crash check and recovery
			WinnowingIntegrity wi = new WinnowingIntegrity();
			Integrity.RecoveryState rs = wi.restore(store);
			if (rs != Integrity.RecoveryState.VALID) {
				Map<Long,BufferRegion> invalid = wi.check(store);
				if (!invalid.isEmpty()) {
					throw new RuntimeIOException(String.format("The BTree required recovery (%s) but there are still %d regions with integrity problems",rs,invalid.size()));
				}
			}
		}

		NodeCapacities nodeCapacities = new RandomAccessNodeCapacities();

		WinnowingContiguousBacking backing = new WinnowingContiguousBacking(store, nodeCapacities, bootstrap, integrity, closeStore);
		BackedBTree btree = new BackedBTree(backing);
		return btree;
	}

}
