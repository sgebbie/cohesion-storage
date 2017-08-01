/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.heap;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.backing.BTreeIndexNode;
import net.gethos.cohesion.storage.backing.BTreeNode;

public class HeapIndexNode extends HeapNode implements BTreeIndexNode, Cloneable {
	
	private static final long NO_SUCH_OFFSET = INVALID_OFFSET;
	private static final long NO_RIGHT_HAND_CHILD = NO_SUCH_OFFSET;
	
	private /* final */ HeapIndexItem[] items;
	
	/**
	 * The child that contains keys strictly greater than all keys in this node.
	 * <p>
	 * Using the 'right-hand-child' mechanism prevents the tree from grown inappropriately in depth
	 * in the case that we insert values in increasing order.
	 */
	protected long rightChild;
	
	public HeapIndexNode(int capacity) {
		super(capacity);
		this.items = new HeapIndexItem[capacity];
		this.rightChild = NO_RIGHT_HAND_CHILD;
	}
	
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder();
		s.append(String.format("{@%8h[t=%s|c=%d|n=%2d]",
				System.identityHashCode(this),
				"INDEX",
				items.length,
				items()
			));
		int n = items();
		for (int i = 0; i < n; i++) {
			String x;
			x = String.format("(@%d:%2d,&%3d)", i, key(i).idHigh, items[i].child);
			s.append(x);
		}
		s.append(String.format("r=%d",rightChild));
		s.append("}");
		return s.toString();
	}
	
	@Override
	public HeapNode clone() {
//		HeapIndexNode copy = new HeapIndexNode(capacity);
//		copy.size = this.size;
//		copy.rightChild = this.rightChild;
		HeapIndexNode copy = (HeapIndexNode)super.clone();
		copy.items = new HeapIndexItem[this.items.length];
		for (int i = 0; i < this.items.length; i++) {
			copy.items[i] = this.items[i] == null ? null : this.items[i].clone();
		}
		return copy;
	}
	
	private boolean hasRightChild() {
		return rightChild != NO_RIGHT_HAND_CHILD;
	}
	
	/**
	 * Obtain the offset of the child node referenced via the item at the given index.
	 * 
	 * @param index
	 * @return offset associated with the child at the given index
	 */
	@Override
	public long offset(int index) {
		if (index >= 0 && index < size) return items[index].child;
		if (index == size) return rightChild;
		return NO_SUCH_OFFSET;
	}
	
	@Override
	public Key key(int idx) {
		if (idx < 0 || idx > size) return null;
		if (idx == size) {
			if (rightChild != NO_RIGHT_HAND_CHILD) {
				return BTree.Key.MAX_KEY;
			} else {
				return null;
			}
		}
		
		return items[idx].key;
	}
	
	@Override
	public boolean writeRight(long offset) {
		this.rightChild = offset;
		return true;
	}
	
	@Override
	public int alloc(Key key) {
		HeapIndexItem index = new HeapIndexItem(key);
		index.key = key;
		index.child = 0;
		return storeItem(this.items, index);
	}
	
	@Override
	public boolean modify(int idx, Key key) {
		if (idx < 0 || idx > size) return false;
		if (idx == size) return true;
		items[idx].key = key;
		return true;
	}

	@Override
	public int children() {
		return size + (rightChild == NO_RIGHT_HAND_CHILD ? 0 : 1);
	}
	
	@Override
	public boolean delete(int idx) {
		if (idx == size && rightChild != NO_RIGHT_HAND_CHILD) {
			this.rightChild = NO_RIGHT_HAND_CHILD;
			if (size > 0) {
				// Promote the right-hand-item to the right-hand-child.
				// Note, this only happens if there was previously a right-hand-child
//				assert(false);
				this.rightChild = items[size-1].child;
				items[size-1] = null;
				size--;
			}
			return true;
		}
		return deleteItem(items, idx);
	}
	
	@Override
	public int find(BTree.Key key) {
		return findItem(this.items, key);
	}
	
	@Override
	public BTreeNodeItem item(int idx) {
		if (idx < 0 || idx > size) return null;
		if (idx == size) {
			HeapIndexItem h = new HeapIndexItem(BTree.Key.MAX_KEY);
			h.child = rightChild;
			return h;
		}
		return items[idx];
	}
	
	// @see net.gethos.cohesion.storage.BTreeIndexNode#write(int, long)
	@Override
	public boolean write(int idx, long data) {
		if (idx >= 0 && idx < size) {
			items[idx].child = data;
			return true;
		}
		if (idx == size) {
			rightChild = data;
			return true;
		}
		return false;
	}

	@Override
	public boolean balance(BTreeNode sibling, boolean requireEmpty) {
		return balance(sibling, null, 0, requireEmpty);
	}
	
	private boolean balance(BTreeNode sibling, final BTree.Key nitemKey, final long nitemChild, boolean requireEmpty) {
		
		// decide which node is left versus right
		HeapIndexNode left;
		HeapIndexNode right;
		int leftRequiredFree;
		int rightRequiredFree;
		HeapIndexNode sl = (HeapIndexNode)sibling;
		assert(!(sl.hasRightChild() && this.hasRightChild()));
		if (sl.children() == 0 || this.hasRightChild()) {
			left = sl;
			leftRequiredFree = 0;
			right = this;
			rightRequiredFree = requireEmpty ? this.items.length : 0;
		} else if (this.children() == 0 || sl.hasRightChild()) {
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
			assert(nitemChild == 0);
		}
		
		// create new left and right buffers
		HeapIndexItem[] bleft = new HeapIndexItem[left.items.length];
		HeapIndexItem[] bright = new HeapIndexItem[right.items.length];

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
		HeapIndexNode src = left;
		int srcIdx = 0;
		int srcCount = src.items();
		
		// destination
		HeapIndexItem[] dst = null;
		int transfered = 0;
		int dstIdx = 0;
		
		int bleftSize = 0;
		int brightSize = 0;
		
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
					lrightChild = right.rightChild;
					rightChildCopied = true;
				} else {
					lrightChild = NO_RIGHT_HAND_CHILD;
					rightChildCopied = false;
				}
				left.rightChild = lrightChild;
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
				right.rightChild = rightChildCopied ? NO_RIGHT_HAND_CHILD : right.rightChild;
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
					dst[dstPos] = new HeapIndexItem(nitemKey);
					dst[dstPos].child = nitemChild;
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
		return ((
					(this.size + ((HeapIndexNode)sibling).size <= this.items.length)
					|| (this.size + ((HeapIndexNode)sibling).size <= ((HeapIndexNode)sibling).items.length)
				) && (
				    !((HeapIndexNode)sibling).hasRightChild() || !this.hasRightChild())
				);
	}
}
