/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.Arrays;

import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.cache.TrivialNodeCache;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * This is a trivial writable transaction for
 * a contiguous store were the allocation values
 * are predetermined.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class TrivialContiguousTransaction extends AbstractWritableContiguousTransaction {

	private final long[] allocationPoints;
	private int allocationPoint;
	
	public TrivialContiguousTransaction(ContiguousStore store, NodeCapacities nodeCapacities, long... allocationPoints) {
		super(store, new TrivialNodeCache(), nodeCapacities);

		this.allocationPoints = Arrays.copyOf(allocationPoints, allocationPoints.length);
	}

	@Override
	long allocateStorage(long capacity) {
		return allocationPoint < allocationPoints.length ? allocationPoints[allocationPoint++] : BTreeBackingTransaction.ALLOC_FAILED;
	}

	@Override
	long freeStorage(long offset, long length) {
		// Note, this trivial transaction simply ignores frees to regions
		//       but this is OK because allocate can never reallocate the same region
		return length;
	}

}
