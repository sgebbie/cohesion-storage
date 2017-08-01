/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;
import net.gethos.cohesion.storage.buffer.BufferRegion;
import net.gethos.cohesion.storage.cache.RegionCache;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * Provides read-only access to tree data held in a contiguous store.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class ReadOnlyContiguousBackingTransaction extends AbstractReadOnlyContiguousBackingTransaction implements ReadOnlyBTreeBackingTransaction {

	private final RegionCache<BufferRegion> unmodifiedNodes;

	public ReadOnlyContiguousBackingTransaction(ContiguousStore store, RegionCache<BufferRegion> nodeCache) {
		super(store);
		this.unmodifiedNodes = nodeCache;
	}

	@Override
	public BTreeNode retrieve(long offset) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open");
		// it should always be safe to simply return fetch(offset) and not cache anything
		BufferRegion n = unmodifiedNodes.get(offset);
		if (n == null) {
			n = fetch(offset);
			if (n == null) return null;
			unmodifiedNodes.cache(offset, n);
		}
		return (BTreeNode)n;
		// Note, this could expose the contents of the cache to modification, thus affecting the cache integrity
		//       so it is important for the caller to honour the read-only nature
	}

}
