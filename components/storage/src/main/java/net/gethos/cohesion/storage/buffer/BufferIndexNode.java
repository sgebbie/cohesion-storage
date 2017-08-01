/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.backing.BTreeIndexNode;
import net.gethos.cohesion.storage.backing.BTreeLeafNode;
import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.StorageConstants;

/**
 * The data part of this node simple stores fixed sized
 * items sequentially. These consist of: (key,child) pairs:
 * <pre>
 * +------+----------------------------------+
 * |header|( key | child )...|(free)...      |
 * +------+----------------------------------+
 * </pre>
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BufferIndexNode extends BufferNode implements BTreeIndexNode {

	private static final int ITEM_OFFSET_CHILD = ITEM_OFFSET_KEY + BTree.Key.SIZE;
	protected static final int ITEM_ENTRY_SIZE = ITEM_OFFSET_CHILD + StorageConstants.SIZEOF_LONG;
	
	public BufferIndexNode(ByteBuffer n) {
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
//			x = String.format("(@%d:%s,&%d)", ipos(i), key(i), child(i));
			x = String.format("(@%d:%2d,&%3d)", ipos(i), key(i).idHigh, child(i));
			s.append(x);
		}
		s.append(String.format("r=%d",rightChild()));
		s.append("}");
		return s.toString();
	}
	
	@Override
	public int children() {
		int size = items();
		long rightChild = rightChild();
		return size + (rightChild == NO_RIGHT_HAND_CHILD ? 0 : 1);
	}
	
	private boolean hasRightChild() {
		return rightChild() != NO_RIGHT_HAND_CHILD;
	}

	@Override
	protected long rightChild() {
		return buffer.getLong(HEADER_OFFSET_RIGHT_HAND_CHILD);
	}

	@Override
	public boolean isFull() {
		return !hasAvailable(ITEM_ENTRY_SIZE);
	}

	@Override
	public boolean modify(int idx, Key key) {
		if (idx < 0) return false;
		int size = items();
		if (idx > size) return false;
		if (idx == size) return true;

		return writeKey(ITEM_ENTRY_SIZE, idx, key);
	}

	@Override
	public Key key(int idx) {
		if (idx < 0) return null;
		int size = items();
		if (idx > size) return null;
		
		if (idx == size) {
			long rightChild = rightChild();
			if (rightChild != NO_RIGHT_HAND_CHILD) {
				return BTree.Key.MAX_KEY;
			} else {
				return null;
			}
		}
		
		return readKey(ITEM_ENTRY_SIZE, idx);
	}

	@Override
	public boolean writeRight(long nOffset) {
		buffer.putLong(HEADER_OFFSET_RIGHT_HAND_CHILD, nOffset);
		return true;
	}

	@Override
	public boolean write(int idx, long data) {
		
//		System.out.printf("index node write [%d] = %d%n",idx,data);
//		System.out.printf("index node before write:%s%n",dump());
//		try {
		if (idx < 0) return false;
		int size = items();
		if (idx < size) {
			buffer.putLong(ipos(idx) + ITEM_OFFSET_CHILD, data);
			return true;
		}
		if (idx == size) {
			writeRight(data);
			return true;
		}
		return false;
//		} finally {
//			System.out.printf("index node after write:%s%n",dump());
//		}
	}

	@Override
	public long offset(int idx) {
		if (idx < 0) return NO_SUCH_OFFSET;
		int size = items();
		if (idx < size) {
			return child(idx);
		}
		if (idx == size) return rightChild();
		return NO_SUCH_OFFSET;
	}

	@Override
	public int alloc(Key key) {
		
//		System.out.printf("index node alloc k.idHigh=%d%n",key.idHigh);
//		System.out.printf("index node before alloc:%s%n",dump());
//		try {
		int idx = find(key);
		if (idx >= 0) return idx;
		int free = free();
		if (free < ITEM_ENTRY_SIZE) {
			// since we are full return (-insertion-1)
			return idx;
		}
		idx = -idx-1;
		
		// now move all data from idx onwards up
		int size = items();
		int start = ipos(idx);		
		int end = ipos(size);
		buffer.position(start);
		buffer.limit(end);
		ByteBuffer upper = buffer.slice();
		
		buffer.limit(buffer.capacity());
		buffer.position(start + ITEM_ENTRY_SIZE);
		buffer.put(upper);
		
		// update header
		free(free-ITEM_ENTRY_SIZE);
		items(size+1);
		
		// copy in the key
		writeKey(ITEM_ENTRY_SIZE, idx, key);
		
		return idx;
//		} finally {
//			System.out.printf("index node after alloc:%s%n",dump());
//		}
	}
	
	@Override
	public boolean delete(int idx) {
		if (idx < 0) return false;
		int size = items();
		if (idx > size) return false;
		int free = free();
		long rightChild = rightChild();
		if (idx == size && rightChild != NO_RIGHT_HAND_CHILD) {
			if (size > 0) {
				// Promote the right-hand-item to the right-hand-child.
				// Note, this only happens if there was previously a right-hand-child
//				assert(false);
				rightChild = child(size-1);
				// shrink
				free(free+ITEM_ENTRY_SIZE);
				items(size-1);
			} else {
				rightChild = NO_RIGHT_HAND_CHILD;
			}
			writeRight(rightChild);
			return true;
		} else {
			// normal delete but moving some data down
			int start = ipos(idx+1);
			int end = ipos(size);
			buffer.position(start);
			buffer.limit(end);
			ByteBuffer upper = buffer.slice();
			
			buffer.clear();
			buffer.position(start - ITEM_ENTRY_SIZE);
			buffer.put(upper);
			
			// update header
			free(free+ITEM_ENTRY_SIZE);
			items(size-1);
			
			return true;
		}
	}
	
	private int ipos(int idx) {
		return (ITEM_ENTRIES_OFFSET + idx*ITEM_ENTRY_SIZE);
	}
	
	private long child(int idx) {
		long child = buffer.getLong(ipos(idx) + ITEM_OFFSET_CHILD);
		return child;
	}
	
	@Override
	public boolean balance(BTreeNode sibling, boolean requireEmpty) {
		return balance(sibling,null,0,requireEmpty ? capacity() - HEADER_SIZE : 0);
	}
	
	private boolean balance(BTreeNode sibling, final BTree.Key nitemKey, final long nitemChild, final int requiredFree) {
		
		// decide which node is left versus right
		BufferIndexNode left;
		BufferIndexNode right;
		int leftRequiredFree;
		int rightRequiredFree;
		BufferIndexNode sl = (BufferIndexNode)sibling;
		assert(!(sl.hasRightChild() && this.hasRightChild()));
		if (sl.children() == 0 || this.hasRightChild()) {
			left = sl;
			leftRequiredFree = 0;
			right = this;
			rightRequiredFree = requiredFree;
		} else if (this.children() == 0 || sl.hasRightChild()) {
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
		}
		
		// create new left and right buffers
		ByteBuffer bleft = ByteBuffer.allocate(left.buffer.capacity());
		ByteBuffer bright = ByteBuffer.allocate(right.buffer.capacity());
		
		// total amount of space available for items, given the free space requirements
		final int lavailable = left.capacity()-HEADER_SIZE;
		final int ravailable = right.capacity()-HEADER_SIZE;
		int available = lavailable+ravailable-leftRequiredFree-rightRequiredFree;
		// amount of data to balance (i.e size of all items, considering both the item data and item headers)
		int outstanding = left.used() + right.used() + (nitemKey == null ? 0 : ITEM_ENTRY_SIZE);
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
		BufferIndexNode src = left;
		int srcIdx = 0;
		int srcCount = src.items();
		
		// destination
		ByteBuffer dst = null;
		int transfered = 0;
		int dstIdx = 0;
		
		boolean allOk = false;
		boolean rightChildCopied = false;
		
		copy:
		while(true) {
						
			// switch destination
			if (dst == null) {
				dst = bleft;
				allOk = false;
			} else if (dst == bleft) {
				
				// initialise header in new left
				long lrightChild;
				if (outstanding == 0) {
					lrightChild = right.rightChild();
					rightChildCopied = true;
				} else {
					lrightChild = NO_RIGHT_HAND_CHILD;
					rightChildCopied = false;
				}
				BufferNode.header(dst, NodeType.INDEX, dst.capacity(), lavailable - transfered, dstIdx, lrightChild);
				
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
				BufferNode.header(dst, NodeType.INDEX, dst.capacity(), ravailable - transfered, dstIdx,
										rightChildCopied ? NO_RIGHT_HAND_CHILD : right.rightChild());
				break copy;
			} else {
				assert(false) : "Unexpected dst state";
			}
			
			// amount of data transfered to the destination
			transfered = 0;
			dstIdx = 0;
			
			filldst:
			while(outstanding > 0) {
				src.buffer.limit(src.capacity());
				
				// get the next item data size
				boolean copyItem;
				if (
						(!ncopied && src == left  && nleft  && srcIdx == nidx)
						||
						(!ncopied && src == right && !nleft && srcIdx == nidx)
					) {
					copyItem = true;
				} else {
					copyItem = false;
					if (srcIdx >= srcCount) {
						assert(src != right);
						src = right;
						srcIdx = 0;
						srcCount = src.items();
					}
				}
				
				int isize = ITEM_ENTRY_SIZE;
				
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
				int dstPos = ipos(dstIdx);
				assert(dstPos + ITEM_ENTRY_SIZE <= dst.capacity());
				dstIdx++;
				
				// perform actual copy
				if (copyItem) {
					
					// insert new item
					dst.position(dstPos);
					nitemKey.write(dst);                         // put key
					dst.putShort(BTreeLeafNode.Flags.NONE.mask); // put flags
					dst.putLong(nitemChild);                     // put child
					
					if (nexists) srcIdx++;
					
					ncopied = true;
				} else {
					int srcPos = src.ipos(srcIdx);
					
					// copy across and update item
					src.buffer.limit(srcPos+ITEM_ENTRY_SIZE);
					src.buffer.position(srcPos);
					dst.position(dstPos);
					dst.put(src.buffer);
					
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
		BufferIndexNode sn = (BufferIndexNode)sibling;
		int outstanding = this.used() + sn.used();
		
		final int tavailable = this.capacity()-HEADER_SIZE;
		final int savailable = sn.capacity()-HEADER_SIZE;
		
		return ((
				(outstanding <= tavailable)
				|| (outstanding <= savailable)
			) && (
			    !this.hasRightChild() || !((BufferIndexNode)sibling).hasRightChild())
			);
	}
}
