/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.backing.BTreeSuperNode;

/**
 * Super node for the tree.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BufferSuperNode extends BufferRegion implements BTreeSuperNode {
	
	private static final int HEADER_OFFSET_MAGIC = HEADER_OFFSET_RESERVED_1;
	private static final int HEADER_OFFSET_DEPTH = HEADER_OFFSET_ITEMS;
	private static final int HEADER_OFFSET_ROOT = HEADER_OFFSET_RIGHT_HAND_CHILD;
	
	static final byte[] MAGIC = {(byte)0x43,(byte)0x48,(byte)0x53}; // CHS - Cohesion Storage
	static final int MAGIC_SIZE = MAGIC.length;
	
	public static final int SUPER_NODE_SIZE = 4096;

	public BufferSuperNode(ByteBuffer n) {
		super(n);
	}
	
	public static BufferSuperNode allocate() {
		ByteBuffer buffer = ByteBuffer.allocate(SUPER_NODE_SIZE);
		buffer.order(StorageConstants.NETWORK_ORDER);
		header(buffer, NodeType.SUPER, SUPER_NODE_SIZE, 0, 0, 0);
		return new BufferSuperNode(buffer);
	}
	
	public static BufferSuperNode wrap(ByteBuffer header) {
		BufferSuperNode sn = new BufferSuperNode(header);
		return sn;
	}
	
	public void stamp(int depth, long root) {
		buffer.position(HEADER_OFFSET_MAGIC);
		buffer.put(MAGIC);
		buffer.putInt(HEADER_OFFSET_CAPACITY, SUPER_NODE_SIZE);
		buffer.putInt(HEADER_OFFSET_DEPTH,depth);
		buffer.putLong(HEADER_OFFSET_ROOT,root);
	}

	/**
	 * Number of levels in the tree
	 */
	@Override
	public int depth() {
		return buffer.getInt(HEADER_OFFSET_DEPTH);
	}
	
	protected void depth(int x) {
		buffer.putInt(HEADER_OFFSET_DEPTH,x);
	}

	@Override
	public long root() {
		return buffer.getLong(HEADER_OFFSET_ROOT);
	}
	
	protected void root(long offset) {
		buffer.putLong(HEADER_OFFSET_ROOT,offset);
	}

	@Override
	public String dump() {
		return String.format("@%8h[t=%s|c=%d|d=%4d|r=0x%8x]",
				System.identityHashCode(buffer),
				type(),
				capacity(),
				depth(),
				root()
			);
	}

}
