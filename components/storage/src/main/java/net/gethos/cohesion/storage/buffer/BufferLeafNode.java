/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.backing.BTreeLeafNode;
import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.StorageConstants;

/**
 * 
 * <pre>
 * +------+----------------------------------+---------+---------+
 * |header|( key | flags | offset | size )...|(free)...|(data)...|
 * +------+----------------------------------+---------+---------+
 * </pre>
 * 
 * Note, the size of the 'offset' and 'size' data could depend on the capacity.
 * That is, if the total node size is &lt;2<sup>15</sup> = 32768 then a <code>short</code>
 * might be used, otherwise an <code>int</code> is used.
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BufferLeafNode extends BufferNode implements BTreeLeafNode {
	
	private static final int ITEM_OFFSET_FLAGS  = ITEM_OFFSET_KEY    + BTree.Key.SIZE;
	private static final int ITEM_OFFSET_OFFSET = ITEM_OFFSET_FLAGS  + StorageConstants.SIZEOF_SHORT;
	private static final int ITEM_OFFSET_SIZE   = ITEM_OFFSET_OFFSET + StorageConstants.SIZEOF_INT;
	public static final int ITEM_ENTRY_SIZE  = ITEM_OFFSET_SIZE   + StorageConstants.SIZEOF_INT;

	public BufferLeafNode(ByteBuffer n) {
		super(n);
	}
	
	@Override
	public String dump() {
		StringBuilder s = new StringBuilder();
		s.append("{");
		s.append(super.dump());
		int n = items();
		for (int i = 0; i < n; i++) {
			String x;
//			x = String.format("(@%d:%2d,%2d,@%2d)", ipos(i), key(i).idHigh, size(i), offset(i));
			x = String.format("(@%4d:%s,%4d,@%4d)", ipos(i), key(i), size(i), offset(i));
//			x = String.format("(@%d,%d,@%d)", ipos(i), size(i), offset(i));
			s.append(x);
		}
		s.append("}");
		return s.toString();
	}
	
	@Override
	public byte flags(int idx) {
		byte flags = buffer.get(ipos(idx) + ITEM_OFFSET_FLAGS);
		return flags;
	}
	
	@Override
	public void flags(int idx, byte flags) {
		buffer.put(ipos(idx) + ITEM_OFFSET_FLAGS,flags);
	}

	@Override
	public int children() {
		int size = buffer.getInt(HEADER_OFFSET_ITEMS);
		return size;
	}

	@Override
	public boolean isFull() {
		return !hasAvailable(ITEM_ENTRY_SIZE);
	}

	@Override
	public boolean modify(int idx, Key key) {
		
		if (idx < 0) return false;
		int size = items();
		if (idx >= size) return false;
		
		return writeKey(ITEM_ENTRY_SIZE, idx, key);
	}

	@Override
	public Key key(int idx) {

		if (idx < 0) return null;
		int size = items();
		if (idx >= size) return null;
		
		return readKey(ITEM_ENTRY_SIZE, idx);
	}
	
	@Override
	public int size(int idx) {

		if (idx < 0) return -1;
		int size = items();
		if (idx > size) return -1;
		
		int isizepos = ipos(idx) + ITEM_OFFSET_SIZE;
		return buffer.getInt(isizepos);
	}
	
	@Override
	protected long rightChild() {
		return NO_RIGHT_HAND_CHILD;
	}

	@Override
	public int realloc(Key key, int length) {
		int idx = find(key);
		if (idx >= 0) {
			return realloc(idx,length);
		} else {
			idx = alloc(-idx-1, length);
			if (idx >= 0) {
				writeKey(ITEM_ENTRY_SIZE, idx, key);
			}
			return idx;
		}
	}

	@Override
	public int realloc(int idx, int length) { 
		int cur = size(idx);
		if (cur < 0) throw new ArrayIndexOutOfBoundsException("The item index does not exist: " + idx + ". Note, there are only: " + items() + " items in the node.");
		if (cur == length) return idx; // Yay! We're done...
		int free = free();
		if (free + cur - length < 0) return -idx-1; // insufficient space
		if (repack(idx,length)) return idx;
		else return -idx-1;
	}
	
	@Override
	public int write(int idx, long objectOffset, ByteBuffer xbuffer) {
		if (idx < 0) return -1;
		int nsize = items();
		if (idx >= nsize) return -1;
		
		int istart = ipos(idx);
		int isize = buffer.getInt(istart + ITEM_OFFSET_SIZE);
		if (isize < objectOffset) return -1;
		int l = Math.min(isize-(int)objectOffset, xbuffer.remaining());
		int dstart = buffer.getInt(istart + ITEM_OFFSET_OFFSET);
		
		//assert(dstart + objectOffset + l < capacity());
		
		buffer.position((int)(dstart + objectOffset));
		int xlimit = xbuffer.limit();
		xbuffer.limit(xbuffer.position() + l);
		buffer.put(xbuffer);
		xbuffer.limit(xlimit);
		return l;
	}

	@Override
	public int read(int idx, long objectOffset, ByteBuffer xbuffer) {
		if (idx < 0) return -1;
		int nsize = items();
		if (idx >= nsize) return -1;
		
		int istart = ipos(idx);
		int isize = buffer.getInt(istart + ITEM_OFFSET_SIZE);
		if (isize < objectOffset) return -1;
		int l = Math.min(isize-(int)objectOffset, xbuffer.remaining());
		int dstart = buffer.getInt(istart + ITEM_OFFSET_OFFSET);
		
		//assert(dstart + objectOffset + l < capacity());
		buffer.position((int)(dstart + objectOffset));
		buffer.limit(buffer.position()+l);
		xbuffer.put(buffer);
		buffer.clear();
		
		return l;
	}
	
	@Override
	public boolean delete(int idx) {
		return repack(idx,-1);
	}
	
	private int ipos(int idx) {
		return ITEM_ENTRIES_OFFSET + idx*ITEM_ENTRY_SIZE;	
	}
	
	private int offset(int idx) {

		if (idx < 0) return -1;
		int size = items();
		if (idx > size) return -1;
		
		int o = ipos(idx) + ITEM_OFFSET_OFFSET;
		return buffer.getInt(o);
	}
	
	private int alloc(int idx, int length) {
		if (idx < 0) return idx;
		int size = items();
		int free = free();
		if (free < (ITEM_ENTRY_SIZE + length)) return -idx-1; // insufficient space
		
		// create space by shifting upper items
		int start = ipos(idx);
		int end = ipos(size);
		buffer.limit(end);
		buffer.position(start);
		ByteBuffer upper = buffer.slice();
		
		buffer.clear();
		buffer.position(start + ITEM_ENTRY_SIZE);
		buffer.put(upper);
		
		// update header
		free(free-(ITEM_ENTRY_SIZE+length));
		items(size+1);
		
		// store the offset and size
		int offset = HEADER_SIZE + size*ITEM_ENTRY_SIZE + free - length;
		buffer.put(start + ITEM_OFFSET_FLAGS, (byte)0);
		buffer.putInt(start + ITEM_OFFSET_OFFSET, offset);
		int isizepos = start + ITEM_OFFSET_SIZE;
		buffer.putInt(isizepos, length);
//		System.out.printf("storing buffer@%h offset@%d=%d size@%d=%d%n",System.identityHashCode(buffer), start + ITEM_OFFSET_OFFSET, offset,isizepos, length);
		
//		dump(System.out);
		
		return idx;
	}
	
	private boolean repack(int idx, int newSize) {
		if (idx < 0) return false;
		int srcCount = items();
		if (idx >= srcCount) return false;
		
		// to delete, we need to re-pack the node because the data may be out of order
		int srcIdx = 0;
		int dstIdx = 0;
		
		ByteBuffer dst = ByteBuffer.allocate(buffer.capacity());
		int dstUsed = 0;
		int dstDataPos = dst.capacity();
		
		while(srcIdx < srcCount) {

			if (srcIdx == idx && newSize < 0) {
				// simply skip, and therefore delete, the item
				srcIdx++;
				continue;
			}
		
			int srcPos = ipos(srcIdx);
			int srcSizePos = srcPos + ITEM_OFFSET_SIZE;
			int srcOffsetPos = srcPos + ITEM_OFFSET_OFFSET;
			int srcSize = this.buffer.getInt(srcSizePos);
			int srcOffset = this.buffer.getInt(srcOffsetPos);
//			System.out.printf("buffer@%h srcLeft=%s srcCount=%d srcIdx=%d srcPos=%d srcSize@%d=%d srcOffset@%d=%d%n",
//						System.identityHashCode(src.buffer),srcLeft,srcCount,srcIdx,srcPos,srcSizePos,srcSize,srcOffsetPos,srcOffset);
			
			int copySize = srcSize;
			int dstSize = srcSize;
			if (srcIdx == idx && newSize >= 0) {
				copySize = Math.min(srcSize, newSize);
				dstSize = newSize;
			}

			// copy across item data
			this.buffer.limit(srcOffset+copySize);
			this.buffer.position(srcOffset);
			dstDataPos -= dstSize;
			dst.position(dstDataPos);
			dst.put(this.buffer);
			
			// copy across and update item
			this.buffer.limit(srcPos+ITEM_ENTRY_SIZE);
			this.buffer.position(srcPos);
			int dstPos = ipos(dstIdx);
			dst.position(dstPos);
			dst.put(this.buffer);
			int dstOffsetPos = dstPos + ITEM_OFFSET_OFFSET;
			dst.putInt(dstOffsetPos, dstDataPos);
			
			if (srcIdx == idx && newSize != srcSize) {
				// fix item size
				int dstSizePos = dstPos + ITEM_OFFSET_SIZE;
				dst.putInt(dstSizePos, newSize);
			}
			
			dstUsed += ITEM_ENTRY_SIZE + dstSize;
			
			dstIdx++;			
			srcIdx++;
			
			this.buffer.clear();
		}
		
		// update the header in the dst
		BufferNode.header(dst, NodeType.LEAF, dst.capacity(), dst.capacity()-HEADER_SIZE-dstUsed, dstIdx, NO_RIGHT_HAND_CHILD);
		swb(dst);
	
		return true;
	}
	
	@Override
	public boolean balance(BTreeNode sibling, boolean empty) {
		return balance(sibling,null,0,empty ? capacity() - HEADER_SIZE : 0);
	}
	
	@Override
	public boolean balance(BTreeNode sibling, final BTree.Key nitemKey, final int nitemSize, boolean empty) {
		return balance(sibling,nitemKey,nitemSize,empty ? capacity() - HEADER_SIZE : 0);
	}

	/**
	 * Balances data between 'this' node and the 'sibling' node, while:
	 * <ul>
	 *   <li>including 'nitemSize' space for a new item indexed by 'nitemKey' if it is not null</li>
	 *   <li>meeting the the minimum 'requiredFree' space in this node</li>
	 *   <li>attempting to have both nodes share the same amount of data</li>
	 * </ul>
	 * 
	 * Thus, if 'requiredFree' was 'capacity-headersize' this would effectively try to push
	 * all data into the sibling.
	 * 
	 * If 'requiredFree' was 0 this would result in standard balancing.
	 * 
	 * If 'nitemKey' was null then only the contents of 'this' and 'sibling' will be accommodated.
	 * 
	 * @param sibling - not to balance with
	 * @param nitemKey - extra key to insert, null if no extra key
	 * @param nitemSize - extra data space to reserve while balancing
	 * @param requiredFree - space to keep free in this node while balancing
	 * @return true if the constraints could be met.
	 */
	protected boolean balance(BTreeNode sibling, final BTree.Key nitemKey, final int nitemSize, final int requiredFree) {
		
		// decide which node is left versus right
		BufferLeafNode left;
		BufferLeafNode right;
		int leftRequiredFree;
		int rightRequiredFree;
		BufferLeafNode sl = (BufferLeafNode)sibling;
		if (sl.children() == 0) {
			left = sl;
			leftRequiredFree = 0;
			right = this;
			rightRequiredFree = requiredFree;
		} else if (this.children() == 0) {
			left = this;
			leftRequiredFree = requiredFree;
			right = sl;
			rightRequiredFree = 0;			
		} else {
			if (this.rightHandKey().compareTo(sibling.rightHandKey()) < 0) {
				left = this;
				leftRequiredFree = requiredFree;
				right = sl;
				rightRequiredFree = 0;
			} else {
				left = sl;
				leftRequiredFree = 0;
				right = this;
				rightRequiredFree = requiredFree;
			}
		}
		
		// find slot for new key relative to left and right
		boolean nexists = false;
		boolean nleft = false;
		boolean ncopied = false;
		int nidx = -1;
		if (nitemKey != null) {
			int lidx = left.find(nitemKey);
			if (lidx >= 0) {
				nidx = lidx;
				nexists = true;
				nleft = true;
			} else {
				int ridx = right.find(nitemKey);
				if (ridx >= 0) {
					// ah - this already exists
					nidx = ridx;
					nexists = true;
					nleft = false;
				} else {
					lidx = -lidx-1;
					ridx = -ridx-1;
					if (ridx == 0) {
						// either inside left, or at start of right,
						// so just assume placement in left
						nidx = lidx;
						nexists = false;
						nleft = true;
					} else {
						// ok, this will be place squarly in the right.
						nidx = ridx;
						nexists = false;
						nleft = false;
					}
				}
			}
		} else {
			assert(nitemSize == 0);
		}
		
		// create new left and right buffers
		ByteBuffer bleft = ByteBuffer.allocate(left.buffer.capacity());
		ByteBuffer bright = ByteBuffer.allocate(right.buffer.capacity());
		
		// total amount of space available for items, given the free space requirements
		final int lavailable = left.capacity()-HEADER_SIZE;
		final int ravailable = right.capacity()-HEADER_SIZE;
		// amount of space available during the copy
		int available = lavailable+ravailable-leftRequiredFree-rightRequiredFree;
		// amount of data to balance (i.e size of all items, considering both the item data and item headers)
		int outstanding = (nitemKey == null ? 0 : ITEM_ENTRY_SIZE + nitemSize) + left.used() + right.used();
		final int balance = outstanding/2;
		
		// check that there is enough space to hold all the data and meet free requirements
		if (available < outstanding) return false;
		
		// check 1/2 full invariant
		// Note, strictly speaking we should only balance if all nodes are kept at least half full.
		// However, given that we have variable length items, we will instead relax the invariant
		// to: don't balance if all the data could fit into a single node.
//		System.err.printf("outstanding=%d lremaining=%d rremainining=%d%n", outstanding, lavailable - leftRequiredFree, ravailable - rightRequiredFree);
//		if ((outstanding <= lavailable - leftRequiredFree) && (outstanding <= ravailable - rightRequiredFree)) return false;
		
		// check variables
		int lremaining;
		int rremaining;
		boolean lok;
		boolean rok;
		
		// source
		BufferLeafNode src = left;
		int srcIdx = 0;
		int srcCount = src.items();
		
		// destination
		ByteBuffer dst = null;
		int transfered = 0;
		int dstDataPos = 0;
		int dstIdx = 0;
		
		boolean allOk = false;
		
		copy:
		while(true) {
			
			// switch destination
			if (dst == null) {
				dst = bleft;
				allOk = false;
			} else if (dst == bleft) {
				
				// initialise header in new left
				BufferNode.header(dst, NodeType.LEAF, dst.capacity(), lavailable - transfered, dstIdx, NO_RIGHT_HAND_CHILD);
				
				// check if we have actually failed
				// update check variables
				lremaining = lavailable - transfered;
				rremaining = ravailable - outstanding;
				lok = lremaining >= leftRequiredFree;
				rok = rremaining >= rightRequiredFree;
				boolean previousRequiredOk = lok && rok;

				if (!allOk && !previousRequiredOk) {
					// oops, we can't fit stuff in correctly
					return false;
				}
				
				dst = bright;
			} else if (dst == bright) {
				// initialise header in new right				
				BufferNode.header(dst, NodeType.LEAF, dst.capacity(), ravailable - transfered, dstIdx, NO_RIGHT_HAND_CHILD);
				break copy;
			} else {
				assert(false) : "Unexpected dst state";
			}
			
			// amount of data transfered to the destination
			transfered = 0;
			dstDataPos = dst.capacity();
			dstIdx = 0;
			
			filldst:
			while(outstanding > 0) {
				src.buffer.limit(src.capacity());
				
				// get the next item data size
				boolean copyItem;
				int dsize;
				if (
						(!ncopied && src == left  && nleft  && srcIdx == nidx)
						||
						(!ncopied && src == right && !nleft && srcIdx == nidx)
					) {
					copyItem = true;
					dsize = nitemSize;
				} else {
					copyItem = false;
					if (srcIdx >= srcCount) {
						assert(src != right);
						src = right;
						srcIdx = 0;
						srcCount = src.items();
					}
					dsize = src.size(srcIdx);
				}
				
				int isize = ITEM_ENTRY_SIZE + dsize;
				
				outstanding -= isize;
				transfered  += isize;
				
				if (dst == bleft) {
					// update check variables
					lremaining = lavailable - transfered;
					rremaining = ravailable - outstanding;
					lok = lremaining >= leftRequiredFree;
					rok = rremaining >= rightRequiredFree;
					boolean requiredOk = lok && rok;
					
					boolean pastBalance = transfered >= balance;
					allOk = requiredOk && pastBalance;
					
					if (!lok) {
						// undo
						outstanding += isize;
						transfered -= isize;
						break filldst;
					}
				} else {
					// when copying to the right we only
					// terminate once all data is transfers
					// (i.e. nothing outstanding)
					allOk = false;
				}

				// leave space for new item data
				dstDataPos -= dsize;
				int dstPos = ipos(dstIdx);
				assert(dstPos + ITEM_ENTRY_SIZE <= dstDataPos);
				dstIdx++;
				
				// perform actual copy
				if (copyItem) {
					
					// insert new item
					dst.position(dstPos);
					nitemKey.write(dst);                         // put key
					dst.putShort(BTreeLeafNode.Flags.NONE.mask); // put flags
					dst.putInt(dstDataPos);                      // put data offset
					dst.putInt(dsize);                           // put data size
					
					if (nexists) srcIdx++;
					
					ncopied = true;
				} else {
					int srcPos = src.ipos(srcIdx);
					int srcOffsetPos = srcPos + ITEM_OFFSET_OFFSET;
					int srcOffset = src.buffer.getInt(srcOffsetPos);
					
					// copy across item data
					src.buffer.limit(srcOffset+dsize);
					src.buffer.position(srcOffset);
					dst.position(dstDataPos);
					dst.put(src.buffer);
					
					int dstOffsetPos = dstPos + ITEM_OFFSET_OFFSET;
					
					// copy across and update item
					src.buffer.limit(srcPos+ITEM_ENTRY_SIZE);
					src.buffer.position(srcPos);
					dst.position(dstPos);
					dst.put(src.buffer);
					dst.putInt(dstOffsetPos, dstDataPos);
					
					srcIdx++;
				}
				
				if (dst == bleft) {
					if(allOk) break filldst;
				}
			}
		}
		
		assert(outstanding == 0);
		
		// copy across newly created data
		left.swb(bleft);
		right.swb(bright);
		
		return true;
	}
	
	@Override
	public boolean isCompressibleWith(BTreeNode sibling) {
		BufferLeafNode sn = (BufferLeafNode)sibling;
		int outstanding = this.used() + sn.used();
		
		final int tavailable = this.capacity()-HEADER_SIZE;
		final int savailable = sn.capacity()-HEADER_SIZE;
		
		return (
				(outstanding <= tavailable)
				|| (outstanding <= savailable)
			   );
	}
}
