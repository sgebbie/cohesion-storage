/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.backing.BTreeLeafNode;
import net.gethos.cohesion.storage.backing.BTreeNode;

public class HeapLeafNode extends HeapNode implements BTreeLeafNode, Cloneable {
	
	private /* final */ HeapLeafItem[] items;
	
	public HeapLeafNode(int capacity) {
		super(capacity);
		this.items = new HeapLeafItem[capacity];
	}
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(String.format("{@%8h[t=%s|c=%d|n=%2d]",
				System.identityHashCode(this),
				"LEAF",
				items.length,
				items()
			));
		int n = items();
		for (int i = 0; i < n; i++) {
			String x;
			x = String.format("(@%4d:%s,%4d)", i, key(i), size(i));
			s.append(x);
		}
		s.append("}");
		return s.toString();
	}
	
	@Override
	public HeapNode clone() {
//		HeapLeafNode copy = new HeapLeafNode(capacity);
//		copy.size = this.size;
		HeapLeafNode copy = (HeapLeafNode)super.clone();
		copy.items = new HeapLeafItem[this.items.length];
		for (int i = 0; i < this.items.length; i++) {
			copy.items[i] = this.items[i] == null ? null : this.items[i].clone();
		}
		return copy;
	}
	
	@Override
	public int realloc(Key key, int length) {
		int idx = findItem(items, key);
		if (idx >= 0) {
			return realloc(idx,length);
		} else {
			HeapLeafItem index = new HeapLeafItem(key,length);
			return storeItem(this.items, index);	
		}
	}
	
	@Override
	public int realloc(int idx, int length) {
		HeapLeafItem item = this.items[idx];
		item.realloc(length);
		return idx;
	}
	
	@Override
	public int size(int idx) {
		return this.items[idx].size;
	}
	
	@Override
	public byte flags(int idx) {
		return this.items[idx].flags;
	}
	
	@Override
	public void flags(int idx, byte flags) {
		this.items[idx].flags = flags;
	}
	
	@Override
	public int children() {
		return size;
	}
	
	@Override
	public boolean delete(int idx) {
		return deleteItem(this.items, idx);
	}
	
	@Override
	public int find(BTree.Key key) {
		return findItem(this.items, key);
	}
	
	@Override
	public BTreeNodeItem item(int idx) {
		return items[idx];
	}

	@Override
	public boolean modify(int idx, Key key) {
		if (idx < 0 || idx >= size) return false;
		items[idx].key = key;
		return true;
	}

	@Override
	public Key key(int idx) {
		if (idx < 0 || idx >= size) return null;
		return items[idx].key;
	}

	@Override
	public int write(int idx, long objectOffset, ByteBuffer buffer) {
		int l = items[idx].data.length - (int)objectOffset;
		if (l <= 0) return 0;
		if (buffer.remaining() < l) l = buffer.remaining();
		
		buffer.get(items[idx].data, (int)objectOffset, l);
		
		return l;
	}

	@Override
	public int read(int idx, long objectOffset, ByteBuffer buffer) {
		int l = items[idx].data.length - (int)objectOffset;
		if (l <= 0) return 0;
		if (buffer.remaining() < l) l = buffer.remaining();
		
		buffer.put(items[idx].data, (int)objectOffset, l);
		
		return l;
	}
	
	@Override
	public boolean balance(BTreeNode sibling, boolean requireEmpty) {
		return balance(sibling,null,0,requireEmpty);
	}
	
	@Override
	public boolean balance(BTreeNode sibling, final BTree.Key nitemKey, final int nitemSize, boolean requireEmpty) {
		
		// decide which node is left versus right
		HeapLeafNode left;
		HeapLeafNode right;
		int leftRequiredFree;
		int rightRequiredFree;
		HeapLeafNode sl = (HeapLeafNode)sibling;
		if (sl.children() == 0) {
			left = sl;
			leftRequiredFree = 0;
			right = this;
			rightRequiredFree = requireEmpty ? this.items.length : 0;
		} else if (this.children() == 0) {
			left = this;
			leftRequiredFree = requireEmpty ? this.items.length : 0;
			right = sl;
			rightRequiredFree = 0;			
		} else {
			if (this.rightHandKey().compareTo(sibling.rightHandKey()) < 0) {
				left = this;
				leftRequiredFree = requireEmpty ? this.items.length : 0;
				right = sl;
				rightRequiredFree = 0;
			} else {
				left = sl;
				leftRequiredFree = 0;
				right = this;
				rightRequiredFree = requireEmpty ? this.items.length : 0;
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
		HeapLeafItem[] bleft = new HeapLeafItem[left.items.length];
		HeapLeafItem[] bright = new HeapLeafItem[right.items.length];

		// total amount of space available for items, given the free space requirements
		final int lavailable = left.items.length;
		final int ravailable = right.items.length;
		// amount of space available during the copy
		int available = lavailable+ravailable-leftRequiredFree-rightRequiredFree;
		// amount of data to balance (i.e size of all items, considering both the item data and item headers)
		int outstanding = left.items() + right.items() + (nitemKey == null ? 0 : 1);
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
		HeapLeafNode src = left;
		int srcIdx = 0;
		int srcCount = src.items();
		
		// destination
		HeapLeafItem[] dst = null;
		int transfered = 0;
		int dstIdx = 0;
		
		int bleftSize = 0;
		int brightSize = 0;
		
		boolean allOk = false;
		
		copy:
		while(true) {
			
			
			// switch destination
			if (dst == null) {
				dst = bleft;
			} else if (dst == bleft) {
				
				// initialise header in new left
				bleftSize = dstIdx;
				
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
				brightSize = dstIdx;
				break copy;
			} else {
				assert(false) : "Unexpected dst state";
			}
			
			// amount of data transfered to the destination
			transfered = 0;
			dstIdx = 0;
			
			filldst:
			while(outstanding > 0) {
				
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
				
				int isize = 1;
				
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
				int dstPos = dstIdx;
				dstIdx++;
				
				// perform actual copy
				if (copyItem) {
					// insert new item
					dst[dstPos] = new HeapLeafItem(nitemKey, nitemSize);
					if (nexists) srcIdx++;
					ncopied = true;
				} else {
					int srcPos = srcIdx;
					// copy across and update item
					dst[dstPos] = src.items[srcPos];
					srcIdx++;
				}
				
				if (dst == bleft) {
					if(allOk) break filldst;
				}
			}
		}
		
		// copy across newly created data
		left.items = bleft;
		left.size = bleftSize;
		right.items = bright;
		right.size = brightSize;
		
		return true;
	}

	@Override
	public boolean isCompressibleWith(BTreeNode sibling) {
		return (
				   (this.size + ((HeapLeafNode)sibling).size <= this.items.length)
				|| (this.size + ((HeapLeafNode)sibling).size <= ((HeapLeafNode)sibling).items.length)
				);
	}
}