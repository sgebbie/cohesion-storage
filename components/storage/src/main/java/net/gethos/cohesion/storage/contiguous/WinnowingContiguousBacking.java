/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;
import net.gethos.cohesion.storage.buffer.BufferRegion;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.cache.BoundedNodeCache;
import net.gethos.cohesion.storage.cache.RegionCache;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * Implements a BTree backing which uses a single backing region stored in a file
 * and additionally maintains its own allocation tree within the tree itself.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class WinnowingContiguousBacking implements BTreeBacking {

	private final ContiguousStore store;
	private final NodeCapacities nodeCapacities;
	private final RegionCache<BufferRegion> nodeCache;
	private final boolean enableIntegrity;
	private final boolean closeStore;

	public WinnowingContiguousBacking(ContiguousStore store, NodeCapacities nodeCapacities, boolean bootstrap, boolean enableIntegrity, boolean closeStore) {
		this.enableIntegrity = enableIntegrity;
		this.closeStore = closeStore;
		this.store = store;
		this.nodeCapacities = nodeCapacities;
		this.nodeCache = new BoundedNodeCache();
		//this.nodeCache = new UnboundedNodeCache();
		//this.nodeCache = new TrivialNodeCache();

		if (bootstrap) {
			AllocationMarker.bootstrap(store, nodeCapacities);
		}

	}

	@Override
	public void close() {
		if (closeStore) store.close();
	}

	@Override
	public BTreeBackingTransaction open() {
		return new WinnowingBackingTransaction(store, nodeCache, nodeCapacities, enableIntegrity);
	}

	@Override
	public ReadOnlyBTreeBackingTransaction openReadOnly() {
		return new ReadOnlyContiguousBackingTransaction(store, nodeCache);
	}

}
