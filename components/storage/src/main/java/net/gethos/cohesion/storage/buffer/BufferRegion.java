/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.zip.Adler32;

import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.backing.BTreeIndexNode;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public abstract class BufferRegion implements Cloneable {

	protected static final int HEADER_OFFSET_NODE_TYPE        = 0;
	protected static final int HEADER_OFFSET_RESERVED_1       = HEADER_OFFSET_NODE_TYPE        + StorageConstants.SIZEOF_BYTE;
	protected static final int HEADER_OFFSET_RESERVED_2       = HEADER_OFFSET_RESERVED_1       + StorageConstants.SIZEOF_BYTE;
	protected static final int HEADER_OFFSET_RESERVED_3       = HEADER_OFFSET_RESERVED_2       + StorageConstants.SIZEOF_BYTE;
	protected static final int HEADER_OFFSET_CHECKSUM         = HEADER_OFFSET_RESERVED_3       + StorageConstants.SIZEOF_BYTE;// CRC32 sum of all data in this node, calculated as if the checksum itself was zero
	protected static final int HEADER_OFFSET_REFCOUNT         = HEADER_OFFSET_CHECKSUM         + StorageConstants.SIZEOF_INT; // Number of nodes refering to this node
	protected static final int HEADER_OFFSET_CAPACITY         = HEADER_OFFSET_REFCOUNT         + StorageConstants.SIZEOF_INT; // size of the whole node in bytes (including header)
	protected static final int HEADER_OFFSET_FREE             = HEADER_OFFSET_CAPACITY         + StorageConstants.SIZEOF_INT; // amount of unallocated bytes in the node
	protected static final int HEADER_OFFSET_ITEMS            = HEADER_OFFSET_FREE             + StorageConstants.SIZEOF_INT; // number of items in the node
	protected static final int HEADER_OFFSET_RIGHT_HAND_CHILD = HEADER_OFFSET_ITEMS            + StorageConstants.SIZEOF_INT;
	protected static final int ITEM_ENTRIES_OFFSET            = HEADER_OFFSET_RIGHT_HAND_CHILD + StorageConstants.SIZEOF_LONG;// the offset of the first item entry

	protected static final int HEADER_OFFSET = HEADER_OFFSET_NODE_TYPE;
	public static final int HEADER_SIZE = ITEM_ENTRIES_OFFSET;

	protected static final int ITEM_OFFSET_KEY = 0;

	protected static final long NO_SUCH_OFFSET = BTreeIndexNode.INVALID_OFFSET;
	protected static final long NO_RIGHT_HAND_CHILD = NO_SUCH_OFFSET;

	protected enum NodeType {

		UNKNOWN((byte)0)
		, INDEX((byte)1)
		, LEAF((byte)2)
		, SUPER((byte)3)
		;

		public final byte code;

		NodeType(byte code) {
			this.code = code;
		}

		public static NodeType valueOf(byte t) {
			if (t == NodeType.UNKNOWN.code) return NodeType.UNKNOWN;
			if (t == NodeType.INDEX.code) return NodeType.INDEX;
			if (t == NodeType.LEAF.code) return NodeType.LEAF;
			if (t == NodeType.SUPER.code) return NodeType.SUPER;
			return NodeType.UNKNOWN;
		}

	}

	protected /* final */ ByteBuffer buffer;

	public BufferRegion(ByteBuffer n) {
		this.buffer = n;
		this.buffer.order(StorageConstants.NETWORK_ORDER);
	}

	@Override
	public BufferRegion clone() {
		BufferRegion x;
		try {
			x = (BufferRegion)super.clone();
			// now copy the buffer
			ByteBuffer xb = x.buffer;
			x.buffer = ByteBuffer.allocate(xb.capacity());
			x.buffer.order(StorageConstants.NETWORK_ORDER);
			xb.rewind();
			x.buffer.put(xb);
			return x;
		} catch (CloneNotSupportedException e) {
			throw new IllegalStateException(e);
		}
	}

	public abstract String dump();

	public static BufferRegion wrapRegion(ByteBuffer n) {
		if (n == null || n.limit() < HEADER_SIZE) return null;
		n.order(StorageConstants.NETWORK_ORDER);
		NodeType t = type(n);
		switch(t) {
			case UNKNOWN: return null;
			case INDEX: return new BufferIndexNode(n);
			case LEAF: return new BufferLeafNode(n);
			case SUPER: return new BufferSuperNode(n);
			default:
				return null;
		}
	}

	public ByteBuffer buffer() {
		return buffer;
	}

	protected static void header(ByteBuffer buffer, NodeType nodeType, int capacity, int free, int items, long rightChild) {
		buffer.put(HEADER_OFFSET_NODE_TYPE, nodeType.code);
		buffer.putInt(HEADER_OFFSET_CHECKSUM, 0);
		buffer.putInt(HEADER_OFFSET_REFCOUNT, 1);
		buffer.putInt(HEADER_OFFSET_CAPACITY, capacity);
		buffer.putInt(HEADER_OFFSET_FREE, free);
		buffer.putInt(HEADER_OFFSET_ITEMS, items);
		buffer.putLong(HEADER_OFFSET_RIGHT_HAND_CHILD, rightChild);
	}

	public NodeType type() {
		return type(buffer);
	}

	public static NodeType type(ByteBuffer n) {
		if (n == null) return NodeType.UNKNOWN;
		byte t = n.get(HEADER_OFFSET_NODE_TYPE);
		return NodeType.valueOf(t);
	}

	protected int capacity() {
		return buffer.getInt(HEADER_OFFSET_CAPACITY);
	}

	/**
	 * Calculate and stamp in the Adler-32 checksum for this node.
	 * 
	 * @return the Adler-32 checksum of this node
	 */
	public int seal() {
		return checksum(this, true);
	}

	/**
	 * Get the checksum recorded for this node.
	 * 
	 * @return the Adler-32 checksum of this node
	 */
	public int checksum() {
		int v = buffer.getInt(HEADER_OFFSET_CHECKSUM);
		return v;
	}

	/**
	 * Calculates the checksum of a node.
	 * 
	 * @param n
	 * @return checksum
	 */
	public static int checksum(BufferRegion n) {
		return checksum(n, false);
	}

	private static int checksum(BufferRegion n, boolean record) {
		Adler32 adler32 = new Adler32();
		int old = record ? 0 : n.buffer.getInt(HEADER_OFFSET_CHECKSUM);
		n.buffer.putInt(HEADER_OFFSET_CHECKSUM, 0);
		assert (n.buffer.hasArray());
		adler32.update(n.buffer.array());
		int v = (int) adler32.getValue();
		n.buffer.putInt(HEADER_OFFSET_CHECKSUM, record ? v : old);
		return v;
	}

	protected void dump(PrintStream out) {
		out.println(dump());
	}

	@Override
	public String toString() {
		return dump();
	}
}
