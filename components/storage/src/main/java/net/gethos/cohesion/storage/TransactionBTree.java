/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;

import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.BTree.Reference;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeIndexNode;
import net.gethos.cohesion.storage.backing.BTreeLeafNode;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * Implements the BTree logic, while abstracting out the backing store.
 * <p>
 * The data access path is:
 * tree -> transaction -> cache -> backing.
 * <p>
 * Note, the tree logic does not hold any state itself. All state
 * is maintained in the backing.
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class TransactionBTree extends ReadOnlyTransactionBTree implements BTreeTransaction {

	private static final long TRANSFER_BLOCK = 4*1024*1024;

	private final BTreeBackingTransaction transaction;

	/**
	 * Note, the backing must contain a representation of
	 * a valid, albeit possibly empty, tree.
	 * 
	 * @param transaction
	 */
	public TransactionBTree(BTreeBackingTransaction transaction) {
		super(transaction);
		this.transaction = transaction;
	}

	// -- convenience

	@Override
	public BTree.Reference truncate(BTree.Reference ref, long length) {
		BTree.Key k = key(ref);
		BTree.Reference r = realloc(transaction, k, length);
		return r;
	}

	@Override
	public BTree.Reference store(BTree.Key key, long objectOffset, ByteBuffer buffer) {
		int rx = buffer.remaining();
		BTree.Reference ref = realloc(transaction, key, objectOffset + rx);
		int r = store(ref,objectOffset,buffer);
		assert(r == rx);
		return ref;
	}

	// -- write

	@Override
	public BTree.Reference truncate(BTree.Key key, long length) {
		BTree.Reference ref = realloc(transaction, key, length);
		return ref;
	}

	@Override
	public int store(BTree.Reference ref, long objectOffset, ByteBuffer buffer) {
		// obtain the leaf
		BTreeLeafNode nl = node(transaction,ref);
		if ((nl.flags(ref.index) & BTreeLeafNode.Flags.EXTERNAL.mask) != 0) {
			ExtranodeReference xr = new ExtranodeReference();
			xr.readFrom(ref.index, nl);

			int rx = buffer.remaining();
			if (xr.size - objectOffset < rx) {
				// first limit the write to the amount available
				int savedLimit = buffer.limit();
				buffer.limit(buffer.position()+(int)(xr.size - objectOffset));
				int l = (int)transaction.write(xr.offset, objectOffset, buffer);
				assert (l == rx) : String.format("l = %d rx = %d", l, rx);
				// restore the saved limit
				buffer.limit(savedLimit);
				return l;
			} else {
				int l = (int)transaction.write(xr.offset, objectOffset, buffer);
				assert(l == rx);
				return l;
			}
		} else {
			int rx = buffer.remaining();
			int r = nl.write(ref.index, objectOffset, buffer);
			assert(r == rx);
			// record changes
			transaction.record(ref.offset,nl);
			return r;
		}
	}

	@Override
	public BTree.Reference delete(BTree.Key key) {
		// Once an item has been removed from a node (and the parent keys updated)
		// the node may contain too few elements. We can correct this by:
		//   1. trying to fill the node by draining elements from one of the node's siblings
		//   2. of if there are not enough elements in the sibling, then
		//      merging the contents with the sibling (since there will be enough space)
		//      and then deleting the node. This might cause a ripple effect up the tree.

		// set up space to record the roots and switch indexes
		BTreeIndexNode[] pathNodes = new BTreeIndexNode[transaction.depth()];
		long[] pathOffsets = new long[pathNodes.length];
		int[] pathIndexes = new int[pathNodes.length];

		// start at root and walk the path through 'depth' steps to the correct leaf
		long nOffset = transaction.root();
		int depth = transaction.depth();
		BTreeNode n = transaction.retrieve(nOffset);
		assert(n != null);
		for (int d = 0; d < depth; d++) {
			BTreeIndexNode c = (BTreeIndexNode)n;
			int x = c.find(key);
			if (x < 0) x = -x-1;

			// record the path for use if balancing is required
			pathOffsets[d] = nOffset;
			pathNodes[d] = c;
			pathIndexes[d] = x;
			nOffset = c.offset(x);
			if (nOffset == BTreeIndexNode.INVALID_OFFSET) return null;
			n = transaction.retrieve(nOffset);
		}

		// check that we're on a leaf
		assert(n instanceof BTreeLeafNode);
		BTreeLeafNode nl = (BTreeLeafNode)n;
		int x = nl.find(key);
		// make sure we are deleting an item that exists
		if (x < 0) return null;

		// simply delete the item from the leaf with no updates to parents etc.
		long s;
		boolean isRightHandItem = nl.isRightHandItem(x);
		if ((nl.flags(x) & BTreeLeafNode.Flags.EXTERNAL.mask) != 0) {
			ExtranodeReference er = new ExtranodeReference();
			er.readFrom(x, nl);
			s = er.size;
			transaction.free(er.offset, er.size);
		} else {
			s = nl.size(x);
		}
		boolean ret = nl.delete(x);
		assert(ret);

		// check if balancing is now required
		balance(transaction, pathOffsets, pathNodes, pathIndexes, depth, nOffset, n, isRightHandItem);

		BTree.Reference ref = new BTree.Reference(nOffset, x, s);
		return ref;
	}

	// -- internals

	private static BTree.Reference realloc(BTreeBackingTransaction t, BTree.Key key, long length) {

		// basic process:
		//   search for node in which to insert data,
		//   consider if each node encountered should be split to make space
		//     if so, then split node and update parent
		//   insert data

		// start at root
		BTreeIndexNode p = null;
		long pOffset = -1;
		long nOffset = t.root();
		BTreeNode n = t.retrieve(nOffset);
		assert(n != null);
		for (int d = 0; d < t.depth(); d++) {
			BTreeIndexNode c = (BTreeIndexNode)n;
			long cOffset = nOffset;

			int x = c.find(key);
			if (x < 0 && c.isFull()) {
				// Opportunistically split the internal index node c.
				// Note, if we split the root, the we must increase the depth
				// and create a new root.
				long siblingOffset = t.alloc(false);
				if (siblingOffset == BTreeBackingTransaction.ALLOC_FAILED) return null;
				BTreeIndexNode sibling = (BTreeIndexNode)t.retrieve(siblingOffset);

				// allocate a new root
				if (cOffset == t.root()) {
					long root = t.alloc(false);
					if (root == BTreeBackingTransaction.ALLOC_FAILED) return null;
					pOffset = root;
					p = (BTreeIndexNode)t.retrieve(root);

					// store a reference in the new root to the old root
					p.writeRight(cOffset);

					t.recordRoot(t.depth() + 1, root);
					d++; // note, we also need to update our depth counter since it is as if we've already visited the root
				}

				// split the current node into two nodes, with a new sibling on the left
				//				System.out.printf("balancing to split root%n");
				//				{
				//					System.out.printf("nl keys before split = ");
				//					for(int i = 0; i < c.children(); i++) System.out.printf("%d ",c.key(i).idHigh);
				//					System.out.printf("%n");
				//				}
				boolean dok = sibling.balance(c, false);
				assert(dok);

				//				{
				//					System.out.printf("sibling keys after split = ");
				//					for(int i = 0; i < sibling.children(); i++) System.out.printf("%d ",sibling.key(i).idHigh);
				//					System.out.printf("%n");
				//
				//					System.out.printf("nl keys after split = ");
				//					for(int i = 0; i < c.children(); i++) System.out.printf("%d ",c.key(i).idHigh);
				//					System.out.printf("%n");
				//				}

				// insert the split reference into the parent (which will have space by now)
				assert(sibling.rightHandKey().compareTo(c.rightHandKey()) < 0) : String.format("sibling not on left");
				assert(p != null);
				int ri = p.alloc(sibling.rightHandKey());
				assert(ri >= 0);
				p.write(ri, siblingOffset);

				t.record(pOffset,p);
				t.record(siblingOffset,sibling);
				t.record(cOffset,c);

				// update the path found so as to follow the correct node after the split
				if (key.compareTo(sibling.rightHandKey()) > 0) {
					x = c.find(key);
				} else {
					x = sibling.find(key);
					c = sibling;
					cOffset = siblingOffset;
				}

			}
			if (x < 0) x = -x-1;

			// find the offset of the child
			nOffset = c.offset(x);

			// check that we actually got a valid node reference back,
			// because, it is possible that if 'x' actually refers the the right-hand-child
			// that there is currently no right-hand-child allocated.
			if (nOffset == BTreeIndexNode.INVALID_OFFSET) {
				//				assert(c.rightChild < 0);
				// allocate a new right-hand-child
				nOffset = t.alloc(d+1 == t.depth());
				if (nOffset == BTreeBackingTransaction.ALLOC_FAILED) return null;
				c.writeRight(nOffset);
				t.record(cOffset,c);
			}

			p = c;
			pOffset = cOffset;

			n = t.retrieve(nOffset);
			assert(n != null);
		}

		// obtain the leaf
		BTreeLeafNode nl = (BTreeLeafNode)n;

		return realloc_provision(t, key, pOffset, p, nOffset, nl, length);
	}

	/**
	 * Provision external or internal space for:
	 * <ul>
	 *   <li>the 'key'</li>
	 *   <li>in leaf 'nl'</li>
	 *   <li>with parent 'p'</li>
	 *   <li>of a given 'length'</li>
	 * </ul>
	 * 
	 * Constraints on tree operations:
	 * <ul>
	 *   <li>raw operations can not be interleaved with internal operations</li>
	 *   <li>internal (re)alloc can be performed, but take care to not reuse the
	 *       reference after other tree ops</li>
	 * </ul>
	 * 
	 * Requirement: decouple raw alloc,free from other ops so that the raw ops
	 * are not interleaved with the internal ops.
	 * 
	 * <pre>
	 *         Process:
	 * 
	 *                                 length > MAX_INTERNAL    or    length <= MAX_INTERNAL
	 *                                 (should be external)           (should be internal)
	 * 
	 *         exists & was external   alloc raw,                     realloc internal,
	 *                                 transfer data,                 copy data to internal,
	 *                                 free old raw,                  free external
	 *                                 search for ref,
	 *                                 update ref
	 * 
	 *         exists & was internal   copy data to buf,              realloc internal
	 *                                 realloc internal as ref,
	 *                                 alloc raw,
	 *                                 copy buf to external,
	 *                                 search for ref,
	 *                                 update ref
	 * 
	 *         does not exist          alloc internal for ref,        alloc internal
	 *                                 alloc raw,
	 *                                 search for ref,
	 *                                 update ref
	 * </pre>
	 */
	private static BTree.Reference realloc_provision(
			final BTreeBackingTransaction t,
			final BTree.Key key,
			final long pOffset, final BTreeIndexNode p,
			final long nOffset, final BTreeLeafNode nl,
			final long length) {
		// Extranode data, supported by maintaining a small buffer stored in the item data
		//                 that then holds the external region offset and size.
		// check if should be stored as extranode data or intranode data
		if (length > t.maxItemData()) {
			// should be external
			return realloc_provision_external(t,key,pOffset,p,nOffset,nl,length);
		} else {
			// should be internal
			return realloc_provision_internal(t,key,pOffset,p,nOffset,nl,length);
		}
	}

	/**
	 * Provision external space, possibly moving data out of internal storage.
	 */
	private static BTree.Reference realloc_provision_external(
			final BTreeBackingTransaction t,
			final BTree.Key key,
			final long pOffset, final BTreeIndexNode p,
			final long nOffset, final BTreeLeafNode nl,
			final long length) {
		// should be external
		final int ni = nl.find(key);

		// Here we always perform external updates after the internal updates that affect structure
		// then search for the internal reference again before updating. This way the two different
		// types of tree operations are not interleaved, and the tree is an a consistent state
		// when switching from one to the other.

		ExtranodeReference xr = null;

		// check if exists
		if (ni >= 0) {
			// check if currently inline
			if ((nl.flags(ni) & BTreeLeafNode.Flags.EXTERNAL.mask) == 0) {
				// exists & was internal
				// copy internal data into a buffer
				// realloc the internal region as a reference
				// alloc a new external region
				// copy the buffer into the external region
				ByteBuffer buffer = ByteBuffer.allocate(nl.size(ni));
				nl.read(ni, 0, buffer);
				buffer.flip();
				Reference r = realloc_inline(t, p, pOffset, nl, nOffset, key, ExtranodeReference.SIZE);
				assert(r != null);
				// --
				long xoffset = t.alloc(length);
				xr = ExtranodeReference.create(xoffset, length);
				t.write(xr.offset, 0, buffer);
			} else {
				// exists & was external
				// fetch the existing reference
				// if the size changes, then:
				//   alloc a new external region
				//   copy the data across
				//   free the existing external region
				final ExtranodeReference xrx = ExtranodeReference.read(ni, nl);
				// --
				if (xrx.size != length) {
					// only allocate a new extent if the sizes differ
					long xoffset = t.alloc(length); // TODO need a realloc here
					xr = ExtranodeReference.create(xoffset, length);
					// copy data from xr to xrp (don't tell anyone)
					transfer(t, xrx, xr);
					t.free(xrx.offset, xrx.size);
				} else {
					xr = xrx;
				}
			}
		} else {
			// does not exist
			// alloc a new internal ref
			// alloc the raw data
			Reference r = realloc_inline(t, p, pOffset, nl, nOffset, key, ExtranodeReference.SIZE);
			assert(r != null);
			// --
			long xoffset = t.alloc(length); // TODO btree extranode: rounding?
			xr = ExtranodeReference.create(xoffset, length);
		}

		// post update of external reference
		// (note, due to structure changes from external alloc/free we must search for the reference again)
		Reference r = null;
		if (xr != null) {
			// search for xref
			r = search(t, key);
			if (r != null) {
				// update xref
				BTreeLeafNode nlr = node(t,r);
				xr.writeTo(r.index, nlr);
				nlr.flags(r.index,BTreeLeafNode.Flags.EXTERNAL.mask);
				r.size = xr.size;
			}
		}

		return r;
	}

	/**
	 * Provision internal space, possibly moving data out of external storage.
	 */
	private static BTree.Reference realloc_provision_internal(
			final BTreeBackingTransaction t,
			final BTree.Key key,
			final long pOffset, final BTreeIndexNode p,
			final long nOffset, final BTreeLeafNode nl,
			final long length) {
		final int ni = nl.find(key);
		// check if exists
		if (ni >= 0) {
			// check if currently internal
			if ((nl.flags(ni) & BTreeLeafNode.Flags.EXTERNAL.mask) == 0) {
				// normal inline realloc
				return realloc_inline(t,p,pOffset,nl,nOffset,key,(int)length);
			} else {
				// realloc inline data
				// copy extranode data into node
				// free extranode data
				ExtranodeReference xr = ExtranodeReference.read(ni, nl);
				Reference r = realloc_inline(t, p, pOffset, nl, nOffset, key, (int)length);
				BTreeLeafNode nlr = node(t,r);
				ByteBuffer buffer = ByteBuffer.allocate((int)(length < xr.size ? length : xr.size));
				t.read(xr.offset, 0, buffer);
				buffer.flip();
				nlr.write(r.index, 0, buffer);
				nlr.flags(r.index,BTreeLeafNode.Flags.NONE.mask);

				// it is safe perform a free of external data
				// since it is after all internal updates have completed.
				t.free(xr.offset, xr.size);
				return r;
			}
		} else {
			// normal inline alloc inline
			return realloc_inline(t,p,pOffset,nl,nOffset,key,(int)length);
		}
	}

	private static Reference realloc_inline(BTreeBackingTransaction t, BTreeIndexNode p, long pOffset, BTreeLeafNode nl, long nOffset, BTree.Key key, int nrequired) {
		int ni = nl.realloc(key, nrequired);

		if (ni < 0) {
			// could not store, presumably insufficient space in the node
			// so create a sibling and to balance with and try again
			// (Could consider a more complete balancing strategy here by first reviewing
			//  the existing sibling nodes to the left and right. This would give a chance
			//  to improve space utilisation - possibly enabling inline data to be larger than
			//  half the size of the node. Note, if we merge to both left and right then our
			//  preemptive allocation in the internal nodes might need to make space for 2 and not
			//  just 1 item.)
			long siblingOffset = t.alloc(true);
			if (siblingOffset == BTreeBackingTransaction.ALLOC_FAILED) return null;
			BTreeLeafNode sibling = (BTreeLeafNode)t.retrieve(siblingOffset);

			BTreeLeafNode target = null;
			long targetOffset = 0;

			// balance by splitting the node into two
			boolean dok = nl.balance(sibling,key,nrequired,false);
			assert(dok);

			// select target
			if (key.compareTo(sibling.rightHandKey()) > 0) {
				target = nl;
				targetOffset = nOffset;
			} else {
				target = sibling;
				targetOffset = siblingOffset;
			}

			ni = target.find(key);
			assert(ni >= 0) : String.format("key=%s ni=%d nrequired=%d from=%d where sibling:%d=%s and nl:%d=%s",key,ni,nrequired,targetOffset,siblingOffset,sibling,nOffset,nl);

			//			{
			//				System.out.printf("nl keys = ");
			//				for(int i = 0; i < nl.children(); i++) System.out.printf("%d ",nl.key(i).idHigh);
			//				System.out.printf("%n");
			//
			//				System.out.printf("sibling keys = ");
			//				for(int i = 0; i < sibling.children(); i++) System.out.printf("%d ",sibling.key(i).idHigh);
			//				System.out.printf("%n");
			//			}

			// create the new parent item and insert
			// (note, the parent would have space for at least one
			//  node, as if it was full it would have been preemptively split)
			int ri = p.alloc(sibling.rightHandKey());
			assert(ri >= 0);
			p.write(ri, siblingOffset);

			// write the data into the node
			BTree.Reference r = new BTree.Reference();
			r.offset = targetOffset;
			r.index = ni;
			r.size = target.size(r.index);

			// note, all these nodes will need to be persisted again
			t.record(nOffset,nl);
			t.record(siblingOffset,sibling);
			t.record(pOffset,p);

			return r;
		} else {
			// write the data into the node
			BTree.Reference r = new BTree.Reference();
			r.offset = nOffset;
			r.index = ni;
			r.size = nl.size(r.index);

			// record changes
			t.record(nOffset,nl);

			// since our realloc may have reduced the size of a variable length
			// item, we need to check if we still meet the half-full invariant,
			// if not, then we need to rebalance.
			if (nl.isHalfEmpty()) {
				// FIXME btree realloc bug: balance may be required after a realloc when size drops
				// it should safe to ignore balancing when depth such that leaf at level 1 in tree
				// this needs to be reviewed, and existing scenarios that trigger this need to be understood :(
				//				System.err.println("hmmmm we need to implement rebalancing on realloc.");
			}

			return r;
		}
	}

	private static void balance(BTreeBackingTransaction t, long[] pathOffsets, BTreeIndexNode[] pathNodes, int[] pathIndexes, int balanceDepth, long nOffset, BTreeNode n, boolean nRightHandKeyChanged) {

		assert(balanceDepth >= 0);

		// check if balancing is now required
		if (balanceDepth > 0) {
			// obtain references relative to the parent
			BTreeIndexNode parent = pathNodes[balanceDepth-1];
			int nIndex = pathIndexes[balanceDepth-1];
			boolean pRightHandKeyChanged = false;

			if (n.isHalfEmpty()) {
				// balancing required for this node at this level
				boolean[] sides = { true, false };

				balanced:
				{
					// lets try see if we can merge and free nodes, which might ripple up the tree
					mergeSides:
						for (boolean isLeft : sides) {
							int siblingIndex = nIndex + (isLeft ? -1 : 1);
							long siblingOffset = parent.offset(siblingIndex);
							if (siblingOffset == BTreeIndexNode.INVALID_OFFSET) continue mergeSides; // it's possible that there is no sibling to the right or left

							BTreeNode sibling = t.retrieve(siblingOffset);

							//						System.err.printf("about to push n=%s sibling=%s%n", n,sibling);
							if (n.isCompressibleWith(sibling) && n.balance(sibling, true)) {
								//							System.err.printf("push succeeded n=%s sibling=%s%n", n,sibling);

								// record the changes at this balance level
								t.free(nOffset);
								t.record(siblingOffset,sibling);

								// check if the change in the parent will affect the right-hand-key of the parent
								pRightHandKeyChanged = parent.isRightHandItem(siblingIndex) || parent.isRightHandItem(nIndex);

								// start rippling up
								// since we removed this node, and balanced with its sibling, make sure we fix keys in the parent
								// (do this before removing the child so that the sibling index is still valid)
								parent.modify(siblingIndex,sibling.rightHandKey());
								// now remove the freed node from the parent
								parent.delete(nIndex);

								break balanced;
							} else {
								//							System.err.printf("push failed n=%s sibling=%s%n", n,sibling);
							}
						}

				// lets try the drain from the left or the right
				drainSides:
					for (boolean isLeft : sides) {
						int siblingIndex = nIndex + (isLeft ? -1 : 1);
						long siblingOffset = parent.offset(siblingIndex);
						if (siblingOffset == BTreeIndexNode.INVALID_OFFSET) continue drainSides; // it's possible that there is no sibling to the right or left

						BTreeNode sibling = t.retrieve(siblingOffset);
						//						System.err.printf("about to pull n=%s sibling=%s on %s%n", n,sibling, isLeft ? "left" : "right");
						if (n.balance(sibling, false)) {
							//							System.err.printf("pull succeeded n=%s sibling=%s%n", n,sibling);

							// record the changes at this balance level
							t.record(nOffset,n);
							t.record(siblingOffset,sibling);

							// check if the change in the parent will affect the right-hand-key of the parent
							pRightHandKeyChanged = parent.isRightHandItem(siblingIndex) || parent.isRightHandItem(nIndex);

							// start rippling up
							// since we balanced this node, make sure we fix keys in parent
							// (ignoring updates to the 'rightChild' since there is no explicit reference key for it)
							parent.modify(siblingIndex, sibling.rightHandKey());
							parent.modify(nIndex, n.rightHandKey());

							break balanced;
						} else {
							//							System.err.printf("pull failed n=%s sibling=%s%n", n,sibling);
						}
					}

						// OK, maybe the tree only has one leaf left in which case it is OK not to balance it
						//  but when it ends up with no contents we should still remove the reference in the
						//  parent and free the node.
						if (t.depth() == 1 && parent.children() == 1) { // TODO btree-balance: review why checking depth?
							if (n.children() == 0) {
								pRightHandKeyChanged = parent.isRightHandItem(nIndex);
								t.free(nOffset);
								parent.delete(nIndex);
							} else {
								// so, maybe the balance (merge/drain/collapse) didn't work
								// but we still need to record any changes
								t.record(nOffset,n);
							}

							break balanced;
						}

						// FIXME btree-balance: hmmm, actually the above only check for siblings that share the same parent
						//                      but we need to consider the case where the potential merge or balance sibling
						//                      does not share the same parent, even though it shares a higher ancestor.
						//                      This might be handled by changing deletion to use preemptive merging as
						//                      per Ohad Rodeh, in B-trees, Shadowing, and Clones (btree_TOS_rodeh.pdf)

						// we should always be able to merge or drain, otherwise one of the other edge cases should have kicked in earlier
						assert(false);
				}

				// now that the parent has been modified, we can balance the parent
				balance(t, pathOffsets, pathNodes, pathIndexes, balanceDepth - 1, pathOffsets[balanceDepth-1], parent, pRightHandKeyChanged);

			} else {
				// no balancing required, but we still need to record this node and we may still need to ripple keys
				t.record(nOffset,n);

				// check if we still need to ripple up...
				if (nRightHandKeyChanged) {
					pRightHandKeyChanged = parent.isRightHandItem(nIndex);
					parent.modify(nIndex, n.rightHandKey());
					balance(t, pathOffsets, pathNodes, pathIndexes, balanceDepth - 1, pathOffsets[balanceDepth-1], parent, pRightHandKeyChanged);
				} else {
					// ripple of balancing has finally stopped
				}
			}
		} else {
			// when the root only references a single child, we should collapse if possible
			assert(balanceDepth == 0 && nOffset == t.root());
			if (t.depth() > 1 && n.children() == 1) {
				assert(n instanceof BTreeIndexNode);
				BTreeIndexNode r = (BTreeIndexNode)n;
				long rOffset = t.root();

				long root = r.offset(0);
				r.delete(0);

				t.recordRoot(t.depth() - 1, root);
				t.free(rOffset);
				assert(root >= 0);
				return;
			} else {
				// tree root has not collapsed, so we still need to record key ripple changes within the root node
				t.record(nOffset,n);
			}
		}
	}

	// -- special purpose

	/**
	 * Performs an in-place replacement of one key with another key if possible.
	 * <p>
	 * This is only valid if there are no keys in the tree that fit in between
	 * the two keys, as then the node structure is unaffected.
	 * 
	 * @param k
	 * @param kprime
	 * @return true if successful
	 */
	public boolean replace(Key k, Key kprime) {

		if (k.compareTo(kprime) == 0) return true;

		// set up space to record the roots and switch indexes
		BTreeIndexNode[] pathNodes = new BTreeIndexNode[transaction.depth()];
		long[] pathOffsets = new long[pathNodes.length];
		int[] pathIndexes = new int[pathNodes.length];

		// start at root and walk the path through 'depth' steps to the correct leaf
		long nOffset = transaction.root();
		int depth = transaction.depth();
		BTreeNode n = transaction.retrieve(nOffset);
		assert(n != null);
		for (int d = 0; d < depth; d++) {
			BTreeIndexNode c = (BTreeIndexNode)n;
			int x = c.find(k);
			if (x < 0) x = -x-1;

			int y = c.find(kprime);
			if (y >= 0) return false; // clearly the replacement already exists in the tree
			if (y < 0) y = -y-1;

			if (x != y) return false; // clearly there must be other keys in between as we've been directed to a different node

			// record the path for use if balancing is required
			pathOffsets[d] = nOffset;
			pathNodes[d] = c;
			pathIndexes[d] = x;
			nOffset = c.offset(x);
			if (nOffset == BTreeIndexNode.INVALID_OFFSET) return false;
			n = transaction.retrieve(nOffset);
		}

		// check that we're on a leaf
		assert(n instanceof BTreeLeafNode);
		BTreeLeafNode nl = (BTreeLeafNode)n;
		int x = nl.find(k);
		// make sure we are replacing an item that exists
		if (x < 0) return false;

		// check that the key on the side if 'k' is not in between kprime and k
		if (kprime.compareTo(k) > 0) {
			BTree.Key kadjacent = nl.key(x+1);
			if (kadjacent != null) {
				if (kadjacent.compareTo(kprime) <= 0) return false;
			}
		} else {
			// i.e. kprime.compareTo(k) < 0
			BTree.Key kadjacent = nl.key(x-1);
			if (kadjacent != null) {
				if (kadjacent.compareTo(kprime) >= 0) return false;
			}
		}

		// make the change
		boolean ok = nl.modify(x, kprime);
		assert(ok);

		transaction.record(nOffset, n);

		// now just need to handle changes to the parent
		for(int d = depth;d>0;d--) {
			long pOffset = pathOffsets[d-1];
			BTreeIndexNode parent = pathNodes[d-1];
			int nIndex = pathIndexes[d-1];
			ok = parent.modify(nIndex, n.rightHandKey());
			assert(ok);
			transaction.record(pOffset,parent);
			boolean pRightHandKeyChanged = parent.isRightHandItem(nIndex);
			if (!pRightHandKeyChanged) {
				break;
			} else {
				n = parent; // ripple change up
			}
		}

		return true;
	}

	/**
	 * Move data from one external region to another.
	 * 
	 * @param t
	 * @param from
	 * @param to
	 */
	private static void transfer(BTreeBackingTransaction t, ExtranodeReference from, ExtranodeReference to) {
		long transferSize = from.size < to.size ? from.size : to.size;
		long transferPos = 0;
		ByteBuffer buffer = ByteBuffer.allocate((int)(transferSize < TRANSFER_BLOCK ? transferSize : TRANSFER_BLOCK));
		while(transferPos < transferSize) {
			t.read(from.offset, transferPos, buffer);
			buffer.flip();
			while(buffer.hasRemaining()) {
				long l = t.write(to.offset, transferPos, buffer);
				transferPos += l;
			}
			buffer.clear();
		}
	}

}
