/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.nio.ByteOrder;

public class BufferScaling {
	
	protected static final ByteOrder NETWORK_ORDER = ByteOrder.BIG_ENDIAN;
	
	/**
	 * Items stored in buffers required approximately 50 bytes at least.
	 * Since some tests defined the capacity in terms of number of items
	 * rather than the number of bytes available in the mode, this can
	 * be used to provide approximate translation.
	 */
	private static final int APPROXIMATE_LEAF_ITEM_SCALING = 100 + BufferLeafNode.ITEM_ENTRY_SIZE;
	private static final int INNER_ITEM_SCALING = BufferIndexNode.ITEM_ENTRY_SIZE;
	private static final int LOWER_ROUNDING = 1024;
	private static final int HIGHER_ROUNDING = 4096;
	
	final int innerCapacity;
	final int leafCapacity;
	
	public BufferScaling(int capacity) {
		this.innerCapacity = round(capacity, INNER_ITEM_SCALING);
		this.leafCapacity = round(capacity, APPROXIMATE_LEAF_ITEM_SCALING);
	}
	
	protected static int round(int c, int itemScaling) {

		int s = c < LOWER_ROUNDING ? BufferNode.HEADER_SIZE + c*itemScaling : c;
		
		if (s <= LOWER_ROUNDING) {
			return s;
		} else {
			return (int)Math.ceil((double)s/(double)HIGHER_ROUNDING)*HIGHER_ROUNDING;
		}
		
	}
	
}
