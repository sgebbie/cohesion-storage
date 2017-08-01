/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.nio.ByteBuffer;
import java.util.Map;

import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.buffer.BufferRegion;
import net.gethos.cohesion.storage.buffer.BufferSuperNode;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.cache.RegionCache;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * Implements tracking and updating of root details
 * for trees stored in a contiguous store.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public abstract class AbstractWritableRootContiguousTransaction extends ReadOnlyContiguousBackingTransaction implements BTreeBackingTransaction {

	public static int PEEK_GUARD = -1;

	private Integer depth;
	private Long root;

	private boolean depthModified;
	private boolean rootModified;
	private boolean superSync;

	final NodeCapacities nodeCapacities;

	final RegionCache<BufferRegion> nodeCache;

	public AbstractWritableRootContiguousTransaction(ContiguousStore store, RegionCache<BufferRegion> nodeCache, NodeCapacities nodeCapacities) {
		super(store, nodeCache);

		this.nodeCapacities = nodeCapacities;
		this.nodeCache = nodeCache;

		this.depth = null;
		this.root = null;
		this.depthModified = false;
		this.rootModified = false;
		this.superSync = true;
	}

	// -- node access

	@Override
	abstract public long alloc(boolean isLeaf);

	@Override
	abstract public long free(long offset);

	@Override
	abstract public void record(long offset, BTreeNode n);

	// -- raw access

	@Override
	abstract public long alloc(long length);

	@Override
	abstract public long free(long offset, long length);

	@Override
	abstract public long write(long offset, long objectOffset, ByteBuffer buffer);

	// -- root access

	@Override
	public boolean commit() {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");

		// double check that the updated root & depth have been synced to the super block
		if (!superSync) return false;

		return super.commit();
	}

	void commit_sealModified(Map<Long, BufferRegion> modifiedNodes) {
		for (BufferRegion r : modifiedNodes.values()) r.seal();
	}

	void commit_writeModified(Map<Long, BufferRegion> modifiedNodes) {
		for(Map.Entry<Long, BufferRegion> x : modifiedNodes.entrySet()) {
			long offset = x.getKey();
			assert(offset>=0);
			BufferRegion n = x.getValue();
			ByteBuffer b = n.buffer();
			b.rewind();
			store.write(offset,b);
			// either we need to invalidate the cache, or simply cache the new version
			//			nodeCache.cache(offset, n);
			nodeCache.invalidate(offset);
		}
		modifiedNodes.clear();
	}

	BufferSuperNode commit_createRoot() {
		// stamping
		if (depthModified || rootModified) {
			BufferSuperNode sn = BufferSuperNode.allocate();
			sn.buffer().rewind();
			store.read(HEADER_OFFSET, sn.buffer());
			sn.stamp(depth,root);
			sn.seal();
			sn.buffer().rewind();
			superSync = true;
			return sn;
		} else {
			return null;
		}
	}

	void commit_writeRoot() {
		// stamping
		BufferSuperNode sn = commit_createRoot();
		if (sn != null) store.write(HEADER_OFFSET,sn.buffer());
	}

	@Override
	public int depth() {
		if (depth == null) {
			depth = super.depth();
		} else {
			if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		}

		return depth;
	}

	@Override
	public long root() {
		if (root == null) {
			root = super.root();
		} else {
			if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		}

		return root;
	}

	@Override
	public void recordRoot(int depth, long root) {
		if (!isOpen()) throw new IllegalStateException("The transaction is no longer open.");
		this.superSync = false;

		if (depth != depth()) {
			this.depthModified = true;
			this.depth = depth;
		}
		if (root != root()) {
			this.rootModified = true;
			this.root = root;
		}
	}

}
