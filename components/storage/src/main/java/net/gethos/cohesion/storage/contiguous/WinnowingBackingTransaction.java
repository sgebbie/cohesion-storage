/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.TransactionBTree;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeIndexNode;
import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.buffer.BufferNode;
import net.gethos.cohesion.storage.buffer.BufferRegion;
import net.gethos.cohesion.storage.buffer.BufferSuperNode;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.cache.RegionCache;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * Implements the virtual winnowing strategy for backing transactions
 * of trees stored in contiguous storage.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class WinnowingBackingTransaction extends AbstractWritableRootContiguousTransaction implements BTreeBackingTransaction {

	/*
	Winnowing Algorithm

	disable tail only allocations mode

	free any removed raw regions

	track allocated chunks

	repeat {
		free any removed nodes
		consider amount of space required for all virtual nodes
		calculate total chunk space allocated

		if under allocated
			allocate chunks of space for batches of nodes
		if over allocated
			free some chunk space

		if no nodes to remove
			enter tail only allocation mode

	} until no nodes to remove and no allocation difference

	calculate remapping by assigning portions of suitable allocated chunks to virtual nodes
	relocate tree references to modified nodes according to node remapping

	# record a list of region locations and lengths that are to be modified
	# create recovery copy of modified regions including header
	# generate a hash of the modified regions including the header
	# generate a hash of the recovery copy including recovery data and recovery hash
	# force flush

	write out all modified regions
	write updated the header root & depth if required
	force flush

	// on start up always check both the recovery region and the last modified regions
	//   if the recovery region is invalid, then assume the tree is valid and continue
	//   if the recovery region is valid, but the tree is invalid then recover
	//   if the recovery region is valid, and the tree is valid then continue
	 */

	private final int MAX_GUARD = 1000;
	private final long BATCH_SIZE = 10*StorageConstants.DEFAULT_NODE_CAPACITY;

	/**
	 * record backed regions that are removed along with their capacities
	 */
	private final Map<Long, Long> removedBackedRegions;

	/**
	 * record (offsets of) "real" backed nodes that are removed along with their capacities
	 */
	private final Map<Long, Integer> removedBackedNodes;

	/**
	 * record virtual and backed nodes that have been allocated or modified
	 */
	private final NavigableMap<Long, BufferRegion> modifiedNodes;

	/**
	 * manages the crash recovery integrity.
	 */
	private final Integrity<BufferRegion> integrity;

	public WinnowingBackingTransaction(ContiguousStore store, RegionCache<BufferRegion> nodeCache, NodeCapacities nodeCapacities, boolean enableIntegrity) {
		super(store, nodeCache, nodeCapacities);

		this.removedBackedRegions = new HashMap<Long, Long>(0);
		this.removedBackedNodes = new HashMap<Long,Integer>();
		this.modifiedNodes = new TreeMap<Long, BufferRegion>();
		this.integrity = enableIntegrity ? new WinnowingIntegrity(this) : new NopIntegrity<BufferRegion>();
	}

	// -- commit

	/**
	 * The basic principle of the winnowing is to convert all updates,inserts,deletes
	 * to only updates.
	 * 
	 * Note, the winnowing strategy is guaranteed to complete because either:
	 * <ul>
	 *   <li>we stop freeing real nodes, or</li>
	 *   <li>we stop allocating new virtual nodes and can remap all said nodes.</li>
	 * </ul>
	 * 
	 * The first halting condition is guaranteed simply because we start with a
	 * finite number of real nodes that can be deleted and we never create new
	 * real nodes during the winnowing.
	 * <p>
	 * The second halting condition is guaranteed because after the first condition
	 * we always enter into tail-only allocation mode. This makes it possible to allocate
	 * space from the end of the allocation tree in a manner that will never cause
	 * new virtual nodes to be created. Note, during the commit phase only the allocation
	 * tree updates could cause new inserts/deletes.
	 */
	@Override
	public boolean commit() {
		checkOpen();

		// disable tail only allocations mode
		boolean tailOnly = false;
		assert(!tailOnly);

		// free any removed raw regions
		// perform any raw region deallocations by updating the allocation tree
		if (!commit_freeRemovedRawRegions()) return false;

		// track alloc chunks
		final Deque<Range> chunks = new ArrayDeque<Range>();

		int guard = 0;
		try {
			winnowing:
				while(true) {
					if (guard++ > MAX_GUARD) throw new IllegalStateException("Winnowing commit seems to have become stuck :(");
					//System.out.printf("winnowing[%d]%n",guard);

					// free any removed "real" nodes
					// (Note, nodes could additionally be removed during allocation/deallocation of chunks)
					if (!removedBackedNodes.isEmpty()) {
						//System.out.printf("nodes to remove=%d%n", removedBackedNodes.size());
						if (!commit_freeRemovedNodes()) return false;
						continue winnowing;
					}

					// calculate discrepancy between chunk allocation and virtual requirements
					final long chunkLength = commit_totalChunkLength(chunks);
					final long virtualLength = commit_totalVirtualLength();
					final long allocationRequirement = virtualLength - chunkLength;

					//System.out.printf("chunkLength=%d virtualLength=%d allocReq=%d%n", chunkLength, virtualLength, allocationRequirement);

					if (allocationRequirement != 0) {

						if (allocationRequirement > 0) {
							// need to create batches and allocate more chunks
							long remaining = allocationRequirement;
							while (remaining > 0) {
								long len = Math.min(remaining, BATCH_SIZE);
								Range r = commit_allocateChunk(tailOnly, len);
								if (r == null) return false;
								chunks.add(r);
								remaining -= len;
							}
						} else {
							// need to deallocate parts of some chunks
							long remaining = -allocationRequirement;
							while (remaining > 0) {
								Range r = chunks.peekLast();
								if (r == null) return false; // oops
								long len = Math.min(r.length, remaining);
								commit_freeChunk(r.offset+r.length-len, len);
								r.length -= len;
								assert(r.length >= 0);
								if (r.length == 0) chunks.removeLast();
								remaining -= len;
							}
						}

						// enter tail only allocation mode
						//System.out.printf("nodes to remove after chunks managment=%d%n", removedBackedNodes.size());
						if (removedBackedNodes.isEmpty()) tailOnly = true; // future allocations will no trigger inserts or deletes
						// XXX review toggle to tailOnly, it might be possible to delay this by one more iteration

						continue winnowing;
					}

					// the winnowing ends because:
					//   - there are no nodes to be removed
					//   - there are no virtual nodes that can not be remapped
					break winnowing;
				}
		} finally {
			if (PEEK_GUARD >= 0 && guard > PEEK_GUARD) {
				PEEK_GUARD=guard;
			}
		}

		assert(removedBackedNodes.isEmpty());
		assert(removedBackedRegions.isEmpty());
		assert(commit_totalChunkLength(chunks) == commit_totalVirtualLength());

		// create remapping by assigning chunks to virtual nodes
		final Map<Long,Long> remapping = commit_createRemapping(chunks);
		if (remapping == null) return false;

		// relocate tree references to modified nodes according to node remapping
		if (!commit_relocate(remapping)) return false;

		// double check that the root is ok, and not virtual
		assert(root() >= 0);

		// include the super node (header with root & depth) if necessary
		BufferSuperNode sn = commit_createRoot();
		if (sn != null) modifiedNodes.put(HEADER_OFFSET, sn);

		if (!modifiedNodes.isEmpty()) {
			// seal or the nodes (record the node checksum)
			commit_sealModified();

			// phase 1: record a recovery backup
			integrity.backup(store, nodeCache, modifiedNodes);

			// perform a write of all modified regions into the backing store
			// Note, when writing out copy of modified data consider tying through to a vectored IO using GatheringByteChannel
			commit_writeModified();
		}

		try {
			return super.commit();
		} finally {
			// phase 2: force flush write
			if (!modifiedNodes.isEmpty()) integrity.commit(store);
		}

	}

	private void commit_sealModified() {
		commit_sealModified(modifiedNodes);
	}

	private Map<Long, Long> commit_createRemapping(Deque<Range> chunks) {
		final Map<Long,Long> remapping = new HashMap<Long, Long>();
		for (Map.Entry<Long, BufferRegion> v : modifiedNodes.headMap(0L).entrySet()) {
			final long vkey = v.getKey();
			if (vkey >= 0L) continue;
			final int len = v.getValue().buffer().capacity();

			Range r = null;
			while (r == null) {
				r = chunks.peekFirst();
				if (r == null) return null; // ran out of chunks
				if (r.length == 0) {
					r = null;
					chunks.removeFirst();
				}
			}

			if (r.length < len) return null; // a chunk that would not be totally filled
			remapping.put(vkey, r.offset);
			r.offset += len;
			r.length -= len;
			assert(r.length >= 0);
		}
		return remapping;
	}

	private long commit_totalChunkLength(Collection<Range> chunks) {
		long length = 0;
		for (Range r : chunks) length += r.length;
		return length;
	}

	private long commit_totalVirtualLength() {
		long length = 0;
		for (Map.Entry<Long, BufferRegion> v : modifiedNodes.headMap(0L).entrySet()) {
			if (v.getKey() >= 0L) continue;
			// sum up virtual node capacities
			length += v.getValue().buffer().capacity();
		}
		return length;
	}

	private void commit_writeModified() {
		commit_writeModified(modifiedNodes);
	}

	private Range commit_allocateChunk(boolean tailOnly, long length) {
		TransactionBTree bt = new TransactionBTree(this);
		Range r = tailOnly ? AllocationMarker.findTail(bt) : AllocationMarker.findFree(bt, 0, length);
		if (r != null) {
			assert(r.length >= length);
			r.length = length;
			boolean ok = AllocationMarker.allocRange(bt, r.offset, r.length);
			if (!ok) r = null;
		}
		return r;
	}

	private boolean commit_freeChunk(long offset, long length) {
		TransactionBTree bt = new TransactionBTree(this);
		boolean ok = AllocationMarker.freeRange(bt, offset, length);
		return ok;
	}

	private boolean commit_freeRemovedNodes() {
		// perform allocation tree updates
		TransactionBTree bt = new TransactionBTree(this);
		// be careful to make a copy so as not to trigger a concurrent modification to removedBackedNodes
		Map<Long, Integer> removing = new HashMap<Long, Integer>(removedBackedNodes);
		for (Map.Entry<Long, Integer> r : removing.entrySet()) {
			boolean ok = AllocationMarker.freeRange(bt, r.getKey(), r.getValue());
			if (!ok) return false;
		}

		// clear handled removals
		for (long offset : removing.keySet()) removedBackedNodes.remove(offset);

		return true;
	}

	private boolean commit_freeRemovedRawRegions() {
		if (!removedBackedRegions.isEmpty()) {
			TransactionBTree bt = new TransactionBTree(this);
			for (Map.Entry<Long, Long> e : removedBackedRegions.entrySet()) {
				boolean ok = AllocationMarker.freeRange(bt, e.getKey(), e.getValue());
				if (!ok) return false;
			}
			removedBackedRegions.clear();
		}
		return true;
	}

	/**
	 * Update all references to the given virtual nodes to point
	 * to the new locations.
	 * <p>
	 * LHS = virtual, RHS = real.
	 * <p>
	 * This includes:
	 * <ul>
	 *   <li>root</li>
	 *   <li>modified index node references to children</li>
	 *   <li>modified set itself</li>
	 * </ul>
	 * 
	 * @param remapping
	 * @return true on success
	 */
	private boolean commit_relocate(Map<Long, Long> remapping) {

		// remap root
		if (root() < 0) {
			Long real = remapping.get(root());
			if (real == null) return false;
			this.recordRoot(depth(), real);
		}

		for (Map.Entry<Long, Long> m : remapping.entrySet()) {
			// remap the modified node
			BufferRegion n = modifiedNodes.remove(m.getKey());
			if (n != null) modifiedNodes.put(m.getValue(), n);

			// remap node references
			// FIXME brute force :(
			for (BufferRegion i : modifiedNodes.values()) {
				if (i instanceof BTreeIndexNode) {
					BTreeIndexNode x = (BTreeIndexNode)i;
					final int children = x.children();
					for (int idx = 0; idx < children; idx++) {
						long was = x.offset(idx);
						if (was == m.getKey()) {
							x.write(idx, m.getValue());
						}
					}
				}
			}
		}

		return true;
	}

	// -- node access

	@Override
	public long alloc(boolean isLeaf) {
		checkOpen();

		//		System.out.printf("WinnowingBackingTransaction.alloc(%s) - start: alloc tree: %s%n", isLeaf, AllocationMarker.toString(new ReadOnlyTransactionBTree(this)));
		//		try {
		// winnowing node allocation
		long nextVirtualId;
		if (!modifiedNodes.isEmpty()) {
			long lowest = modifiedNodes.navigableKeySet().first();
			nextVirtualId = lowest < 0 ? lowest -1 : -1;
		} else {
			nextVirtualId = -1;
		}
		assert(nextVirtualId < 0);
		BufferNode n = BufferNode.allocate(nodeCapacities.capacity(isLeaf), isLeaf);
		modifiedNodes.put(nextVirtualId, n);
		return nextVirtualId;
		//		} finally {
		//			System.out.printf("WinnowingBackingTransaction.alloc(%s) - end: alloc tree: %s%n", isLeaf, AllocationMarker.toString(new ReadOnlyTransactionBTree(this)));
		//		}
	}

	@Override
	public long free(long offset) {
		checkOpen();

		// winnowing free
		//		System.out.printf("WinnowingBackingTransaction.free(%d) - start: alloc tree: %s%n", offset, AllocationMarker.toString(new ReadOnlyTransactionBTree(this)));
		//		try {

		if (offset < 0) {
			// free the virtual node
			BufferRegion n = modifiedNodes.remove(offset);
			if (n == null) throw new IllegalStateException(String.format("The virtual node @%d can not be freed if it has not been created within the context of this transaction.", offset));
			else return n.buffer().capacity();
		} else {
			if (removedBackedNodes.containsKey(offset)) throw new IllegalStateException(String.format("The backed node @%d can not be freed as it has already been freed within the context of this transaction.", offset));

			BufferRegion n = modifiedNodes.remove(offset);
			if (n == null) n = (BufferNode)retrieve(offset);

			if (n != null) {
				removedBackedNodes.put(offset,n.buffer().capacity());
				return n.buffer().capacity();
			} else {
				return -1;
			}

		}

		//		} finally {
		//			System.out.printf("WinnowingBackingTransaction.free(%d) - end: alloc tree: %s%n", offset, AllocationMarker.toString(new ReadOnlyTransactionBTree(this)));
		//		}
	}

	@Override
	public void record(long offset, BTreeNode n) {
		checkOpen();
		// winnowing node write
		//		System.out.printf("WinnowingBackingTransaction.record(%d, %s) - start: alloc tree: %s%n", offset, n, AllocationMarker.toString(new ReadOnlyTransactionBTree(this)));
		//		try {

		if (offset >= 0 && removedBackedNodes.containsKey(offset)) throw new IllegalStateException(String.format("The backed node @%d can not be recorded as it has already been freed within the context of this transaction.", offset));

		modifiedNodes.put(offset, (BufferNode)n);
		//		} finally {
		//			System.out.printf("WinnowingBackingTransaction.record(%d, %s) - end: alloc tree: %s%n", offset, n, AllocationMarker.toString(new ReadOnlyTransactionBTree(this)));
		//		}
	}

	/**
	 * First look in the modified set,
	 * otherwise go to "the source".
	 */
	@Override
	public BTreeNode retrieve(long offset) {
		checkOpen();

		// winnowing node read
		BTreeNode n = (BTreeNode)modifiedNodes.get(offset);
		if (n == null) {
			n = super.retrieve(offset);
			// create a clone that can be modified
			// this ensures that the cache is left unaffected
			if (n != null) {
				n = ((BufferNode)n).clone();
			}
		}

		return n;
	}

	// -- raw access

	@Override
	public long alloc(long length) {
		checkOpen();

		// winnowing raw allocation
		//		System.out.printf("WinnowingBackingTransaction.alloc(%d) - start: alloc tree: %s%n", length, AllocationMarker.toString(new ReadOnlyTransactionBTree(this)));
		//		try {

		// Note, this process accesses and updates the allocation tree within the context of the transaction,
		// however, we only do this free allocations and not for deallocations. As a result, we can be sure that
		// we will only allocate underlying space that was free at the start of the transaction, and furthermore,
		// we will not double allocate space since it is allocations within the context of this transaction are
		// tracked within the transaction context version of the tree. Finally, since deallocations are delayed
		// if the transaction is rolled back we can be sure we never accidentally overwrote space that contained
		// data that should be retained after the transaction roll back. Note, these operations to not trigger
		// a commit.

		// search for a region in the underlying allocation tree and allocate it
		TransactionBTree bt = new TransactionBTree(this);
		Range r = AllocationMarker.findFree(bt, 0, length);
		if (r == null) {
			return BTreeBackingTransaction.ALLOC_FAILED;
		} else {
			// update the allocation tree within the context of this transaction itself
			assert(r.length >= length);
			boolean ok = AllocationMarker.allocRange(bt, r.offset, length);
			if (!ok) {
				return BTreeBackingTransaction.ALLOC_FAILED;
			}
			return r.offset;
		}

		//		} finally {
		//			System.out.printf("WinnowingBackingTransaction.alloc(%d) - end: alloc tree: %s%n", length, AllocationMarker.toString(new ReadOnlyTransactionBTree(this)));
		//		}
	}

	@Override
	public long free(long offset, long length) {
		checkOpen();
		// winnowing raw free

		// TODO read allocation tree to confirm that this region was in fact allocated

		removedBackedRegions.put(offset, length);

		return length;
	}

	@Override
	public long write(long offset, long objectOffset, ByteBuffer buffer) {
		checkOpen();

		if (offset < 0) throw new IllegalStateException(String.format("Bad offset @%d for raw write", offset));

		// winnowing raw write

		return store.write(offset+objectOffset, buffer); // Note, this does not provide sufficient atomicity and isolation
	}

	// -- internal

	private void checkOpen() {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
	}

}
