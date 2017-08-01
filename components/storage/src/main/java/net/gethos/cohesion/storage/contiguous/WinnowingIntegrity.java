/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTrees;
import net.gethos.cohesion.storage.ReadOnlyBTreeTransaction;
import net.gethos.cohesion.storage.ReadOnlyTransactionBTree;
import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;
import net.gethos.cohesion.storage.buffer.BufferNode;
import net.gethos.cohesion.storage.buffer.BufferRegion;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * Implements integrity management for the winnowing transaction process.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class WinnowingIntegrity extends RegionIntegrity<BufferRegion> implements Integrity<BufferRegion> {

	/**
	 * Create an integrity instance suitable for checking and recovery, but not for backup.
	 */
	public WinnowingIntegrity() {
		super(new BufferNodeByteBufferAccessor());
	}

	/**
	 * Create an integrity instance with access to the tree for allocation information
	 * so as to support the backup process.
	 */
	public WinnowingIntegrity(ReadOnlyBTreeBackingTransaction robt) {
		super(
				new BufferNodeByteBufferAccessor(),
				new WinnowingAllocation(robt)
		);
	}

	@Override
	public Map<Long, BufferRegion> check(ContiguousStore store) {
		final Map<Long, BufferRegion> invalid = new HashMap<Long, BufferRegion>();

		// visit every node in the tree and verify the checksum
		BTree bt = BTrees.newInstance(store,false,false);
		try {
			ReadOnlyBTreeTransaction t = bt.openReadOnly();
			try {
				((ReadOnlyTransactionBTree)t).visit(new ReadOnlyTransactionBTree.BTreeNodeVisitor() {
					@Override
					public void visit(int level, long offset, BTreeNode n) {
						BufferRegion r = (BufferNode)n;
						// obtain the recorded checksum
						int currentChecksum = r.checksum();
						// recalculate the region checksum
						int calculatedChecksum = BufferRegion.checksum(r);
						// compare checksums for validity
						if (currentChecksum != calculatedChecksum) invalid.put(offset, r);
					}
				});
			} finally {
				t.close();
			}
		} finally {
			bt.close();
		}

		return invalid;
	}

	private static class BufferNodeByteBufferAccessor implements RegionIntegrity.ByteBufferAccessor<BufferRegion> {
		@Override
		public ByteBuffer buffer(BufferRegion t) {
			return t == null ? null : t.buffer();
		}
	}

	private static class WinnowingAllocation implements RegionIntegrity.AllocationAccessor {

		private final ReadOnlyTransactionBTree bt;

		public WinnowingAllocation(ReadOnlyBTreeBackingTransaction robt) {
			this.bt = robt == null ? null : new ReadOnlyTransactionBTree(robt);
		}

		@Override
		public long tail() {
			Range r = AllocationMarker.findTail(bt);
			return r == null ? -1 : r.offset;
		}

	}
}
