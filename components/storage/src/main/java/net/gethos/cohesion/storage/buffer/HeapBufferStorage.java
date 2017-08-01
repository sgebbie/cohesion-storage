/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapBufferStorage {
	
	protected static final ByteOrder NETWORK_ORDER = ByteOrder.BIG_ENDIAN;
	
	private final Map<Long, ByteBuffer> buffers;
	private long freeOffsetHint;
	
	/**
	 * This holds the depth of the leaf nodes, where the depth of the root == 0.
	 */
	public int depth;
	
	/**
	 * The offset of the current root node.
	 */
	public long root;
	
	private final NodeCapacities nodeCapacities;
	
	public HeapBufferStorage(NodeCapacities nodeCapacities) {
		this.nodeCapacities = nodeCapacities;
		this.buffers = new HashMap<Long, ByteBuffer>();
		this.depth = 0;
		this.root = 0;
		this.freeOffsetHint = 0;
		
		initialise();
	}
	
	private void initialise() {
		int depth = 1;
		long root = alloc(false);
		if (root < 0) throw new OutOfMemoryError("Unable to allocate a root for the BTree");
		recordRoot(depth,root);
	}
	
	public void dump(PrintStream out) {
		SortedSet<Long> offsets = new TreeSet<Long>();
		offsets.addAll(buffers.keySet());
		for (long o : offsets) {
			BTreeNode n = retrieve(o);
			if (n instanceof BufferNode) {
				out.printf("o=%3d", o);
				((BufferNode) n).dump(out);
			}
		}
	}
	
	public int depth() {
		return depth;
	}
	
	public long root() {
		return root;
	}

	/**
	 * Write the super blocks with the reference to the root node.
	 * <p>
	 * This should include the root offset, tree depth, node capacity parameter. 
	 */
	public void recordRoot(int depth, long root) {
		this.depth = depth;
		this.root = root;
	}

	public void record(long offset, BTreeNode n) {
		ByteBuffer b = buffers.put(offset, ((BufferNode)n).buffer);
		assert(b != null); // we should only record buffers that where previously retrieved.
		// Note, however, that the exact buffer may be swapped out in the node, hence we still need to put it back again.
	}

	public BTreeNode retrieve(long offset) {
		ByteBuffer n = buffers.get(offset);
		return BufferNode.wrap(n);
	}
	
	public long alloc(boolean isLeaf) {
		return isLeaf
			? allocLeaf()
			: allocIndex();
	}

	private long allocIndex() {
		long l = allocBuffer(nodeCapacities.innerCapacity());
		if (l == BTreeBackingTransaction.ALLOC_FAILED) return l;
		ByteBuffer n = buffers.get(l);
		BufferNode.initialiseIndex(n);
		return l;
	}

	private long allocLeaf() {
		long l = allocBuffer(nodeCapacities.leafCapacity());
		if (l == BTreeBackingTransaction.ALLOC_FAILED) return l;
		ByteBuffer n = buffers.get(l);
		BufferNode.initialiseLeaf(n);
		return l;
	}
	
	private long allocBuffer(int bufferCapacity) {
		ByteBuffer n = ByteBuffer.allocate(bufferCapacity);
		n.order(NETWORK_ORDER);
		long l;
		int wrap = 0;
		for (l = freeOffsetHint; buffers.containsKey(l); l++) {
			if (l == Long.MAX_VALUE) {
				// loop back to beginning and search from 0
				l = -1;
				wrap++;
				if (wrap >= 2) return BTreeBackingTransaction.ALLOC_FAILED;
			}
		}
		buffers.put(l, n);
		freeOffsetHint = l+1;
		return l;
	}

	public long free(long offset) {
		return free(offset,-1);
	}
	
	public long free(long offset, long length) {
		ByteBuffer n = buffers.remove(offset);
		if (n == null) {
			return -1;
		} else {
			freeOffsetHint = offset;
			int l = n.capacity();
			if (length >= 0) assert(length == l);
			return l;
		}
	}

	public long alloc(long length) {
		if (length > Integer.MAX_VALUE) return BTreeBackingTransaction.ALLOC_FAILED;
		return allocBuffer((int)length);
	}

	public long read(long offset, long objectOffset, ByteBuffer buffer) {
		ByteBuffer n = buffers.get(offset);
		if (n == null) return 0;
		if (objectOffset > n.capacity()) return 0;
		n.clear();
		n.position((int)objectOffset);
		int l = Math.min(n.remaining(), buffer.remaining());
		n.limit(n.position() + l);
		buffer.put(n);
		n.clear();
		return l;
	}
	
	public long write(long offset, long objectOffset, ByteBuffer buffer) {
		ByteBuffer n = buffers.get(offset);
		if (n == null) return 0;
		if (objectOffset > n.capacity()) return 0;
		n.clear();
		n.position((int)objectOffset);
		int l = Math.min(n.remaining(), buffer.remaining());
		buffer.limit(l);
		n.put(buffer);
		n.clear();
		return l;
	}

}
