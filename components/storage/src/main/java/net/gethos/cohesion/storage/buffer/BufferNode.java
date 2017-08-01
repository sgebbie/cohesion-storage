/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.StorageConstants;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * This nodes store items directly into a buffer.
 * <p>
 * The basic format is:
 * <pre>
 * +------+------------------------+-------------+---------------------------+
 * |header|(fixed length item data)|free space...|(variable length item data)|
 * +------+------------------------+-------------+---------------------------+
 * </pre>
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 */
public abstract class BufferNode extends BufferRegion implements BTreeNode {

	public BufferNode(ByteBuffer n) {
		super(n);
	}

	protected abstract long rightChild();

	@Override
	public String dump() {
		return String.format("@%8h[t=%s|c=%d|f=%3d|n=%2d]",
				System.identityHashCode(buffer),
				type(),
				capacity(),
				free(),
				items()
		);
	}

	@Override
	public BufferNode clone() {
		return (BufferNode)super.clone();
	}

	@Override
	public abstract boolean balance(BTreeNode sibling, boolean requireEmpty);

	public static int capacity(ByteBuffer n) {
		if (n == null) return -1;
		return n.getInt(HEADER_OFFSET_CAPACITY);
	}

	public static BufferNode wrap(ByteBuffer n) {
		return (BufferNode)wrapRegion(n);
	}

	public static BufferNode allocate(int capacity, boolean isLeaf) {
		if (isLeaf) return allocateLeaf(capacity);
		else return allocateIndex(capacity);
	}

	public static BufferLeafNode allocateLeaf(int capacity) {
		ByteBuffer buffer = ByteBuffer.allocate(capacity);
		initialise(buffer, NodeType.LEAF);
		return new BufferLeafNode(buffer);
	}

	public static BufferIndexNode allocateIndex(int capacity) {
		ByteBuffer buffer = ByteBuffer.allocate(capacity);
		initialise(buffer, NodeType.INDEX);
		return new BufferIndexNode(buffer);
	}

	public static void initialiseLeaf(ByteBuffer buffer) {
		initialise(buffer,NodeType.LEAF);
	}

	public static void initialiseIndex(ByteBuffer buffer) {
		initialise(buffer, NodeType.INDEX);
	}

	@Override
	public boolean isRightHandItem(int idx) {
		return idx == items() - 1;
	}

	@Override
	public BTree.Key rightHandKey() {
		return key(items()-1);
	}

	@Override
	public int find(Key key) {

		int size = items();
		if (size < 9) {//Integer.MAX_VALUE) {
			// perform a linear search within the node to find the insertion point
			int insertion = 0;
			for (insertion = 0; insertion < size; insertion++) {
				Key ikey = key(insertion);
				int c = ikey.compareTo(key);
				if (c == 0) {
					// if the key exists then return the position
					return insertion;
				}
				if (c > 0) break; // found the first key larger than the key being searched for
			}

			// the key does not exist, so return the position that it would be inserted into
			return (-(insertion) - 1);
		} else {
			// perform a binary search for the item
			int min = 0;
			int max = size-1;
			while(min <= max) {
				int mid = min + ((max - min) / 2);
				Key ikey = key(mid);
				int c = key.compareTo(ikey);
				if (c == 0) return mid;
				if (c > 0) {
					min = mid + 1;
				} else {
					max = mid - 1;
				}
			}
			return (-(min)-1);
		}
	}

	@Override
	public boolean isHalfEmpty() {
		int free = free();
		int used = (capacity() - HEADER_SIZE) - free;
		return used < free;
	}

	/**
	 * Switches data from <code>updated</code> into the node buffer.
	 */
	final void swb(ByteBuffer updated) {
		// (Note, the strategy can affect performance in either direction
		//        and depends on slight support from the store in the case
		//        where the buffers are simple taken from the heap)

		// copy the new data into the buffer
		//		this.buffer.clear();
		//		updated.clear();
		//		this.buffer.put(updated);

		// simply switch the buffers
		this.buffer = updated;
	}

	private static void initialise(ByteBuffer buffer, NodeType nodeType) {
		buffer.order(StorageConstants.NETWORK_ORDER);
		header( buffer,
				nodeType,
				buffer.capacity(),
				buffer.capacity() - HEADER_SIZE,
				0,
				NO_RIGHT_HAND_CHILD
		);
	}

	protected boolean hasAvailable(int size) {
		return size <= free();
	}

	/**
	 * @return data space used (i.e. capacity used excluding header size)
	 */
	protected int used() {
		return (capacity() - HEADER_SIZE) - free();
	}

	/**
	 * @return the total amount of free space left in the node
	 */
	protected int free() {
		return buffer.getInt(HEADER_OFFSET_FREE);
	}

	protected void free(int x) {
		buffer.putInt(HEADER_OFFSET_FREE,x);
	}

	/**
	 * number of explicit items in the node
	 * (i.e. excluding the potential right-child in an index node)
	 */
	protected int items() {
		return buffer.getInt(HEADER_OFFSET_ITEMS);
	}

	protected void items(int x) {
		buffer.putInt(HEADER_OFFSET_ITEMS,x);
	}

	protected boolean writeKey(int itemEntrySize, int idx, Key key) {
		buffer.position((ITEM_ENTRIES_OFFSET + idx*itemEntrySize) + ITEM_OFFSET_KEY);
		key.write(buffer);
		return true;
	}

	protected Key readKey(int itemEntrySize, int idx) {
		buffer.position((ITEM_ENTRIES_OFFSET + idx*itemEntrySize) + ITEM_OFFSET_KEY);
		return new BTree.Key(buffer);
	}

}

