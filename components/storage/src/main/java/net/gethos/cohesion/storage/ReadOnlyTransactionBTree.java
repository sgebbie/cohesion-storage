/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import net.gethos.cohesion.storage.backing.BTreeIndexNode;
import net.gethos.cohesion.storage.backing.BTreeLeafNode;
import net.gethos.cohesion.storage.backing.BTreeNode;
import net.gethos.cohesion.storage.backing.ReadOnlyBTreeBackingTransaction;

public class ReadOnlyTransactionBTree implements ReadOnlyBTreeTransaction {

	private final ReadOnlyBTreeBackingTransaction transaction;

	/**
	 * Note, the backing must contain a representation of
	 * a valid, albeit possibly empty, tree.
	 * 
	 * @param transaction
	 */
	public ReadOnlyTransactionBTree(ReadOnlyBTreeBackingTransaction transaction) {
		this.transaction = transaction;
	}

	@Override
	public boolean close() {
		transaction.close();
		return true;
	}

	@Override
	public boolean isOpen() {
		return transaction.isOpen();
	}

	@Override
	public boolean commit() {
		return transaction.commit();
	}

	@Override
	public BTree.Stat stat(BTree.Key key) {
		BTree.Reference ref = search(key);
		if (ref == null) return null;

		BTree.Stat stat = new BTree.Stat();
		stat.itemNodeOffset = ref.offset;
		stat.itemIndex = ref.index;

		// obtain the leaf
		BTreeLeafNode nl = node(transaction,ref);
		stat.itemSize = nl.size(ref.index);
		stat.itemFlags = nl.flags(ref.index);

		if ((stat.itemFlags & BTreeLeafNode.Flags.EXTERNAL.mask) != 0) {
			ExtranodeReference xr = new ExtranodeReference();
			xr.readFrom(ref.index, nl);

			stat.externalOffset = xr.offset;
			stat.externalSize = xr.size;

		} else {
			stat.externalOffset = -1;
			stat.externalSize = -1;
		}

		return stat;
	}

	@Override
	public BTree.Reference search(BTree.Key key) {
		return search(transaction, key);
	}

	protected static BTree.Reference search(final ReadOnlyBTreeBackingTransaction t, BTree.Key key) {
		// start at root and find the correct leaf
		long nOffset = t.root();
		int depth = t.depth();
		BTreeNode n = t.retrieve(nOffset);
		assert(n != null);
		for (int d = 0; d < depth; d++) {
			BTreeIndexNode c = (BTreeIndexNode)n;
			int x = c.find(key);
			if (x < 0) x = -x-1;
			nOffset = c.offset(x);
			if (nOffset == BTreeIndexNode.INVALID_OFFSET) return null;
			n = t.retrieve(nOffset);
		}

		// check that we're on a leaf
		assert(n != null);
		assert(n instanceof BTreeLeafNode);
		BTreeLeafNode nl = (BTreeLeafNode)n;
		int x = nl.find(key);
		if (x < 0) return null;

		if ((nl.flags(x) & BTreeLeafNode.Flags.EXTERNAL.mask) != 0) {
			ExtranodeReference er = new ExtranodeReference();
			er.readFrom(x, nl);
			return new BTree.Reference(nOffset, x, er.size);
		} else {
			return new BTree.Reference(nOffset, x, nl.size(x));
		}
	}

	@Override
	public int fetch(BTree.Reference ref, long objectOffset, ByteBuffer buffer) {
		BTreeLeafNode h = node(transaction, ref);
		if ((h.flags(ref.index) & BTreeLeafNode.Flags.EXTERNAL.mask) != 0) {
			ExtranodeReference er = new ExtranodeReference();
			er.readFrom(ref.index, h);
			int br = buffer.remaining();
			if (er.size - objectOffset < br) {
				// first limit the read to the amount available
				int savedLimit = buffer.limit();
				//				buffer.limit((savedLimit-br)+(int)(er.size-objectOffset));
				buffer.limit(buffer.position()+(int)(er.size - objectOffset));
				int l = (int)transaction.read(er.offset, objectOffset, buffer);
				// restore the saved limit
				buffer.limit(savedLimit);
				return l;
			} else {
				int l = (int)transaction.read(er.offset, objectOffset, buffer);
				return l;
			}
		} else {
			// simply read the inline data as per usual
			int l = h.read(ref.index, objectOffset, buffer);
			return l;
		}
	}

	@Override
	public BTree.Key key(BTree.Reference ref) {
		BTreeLeafNode h = node(transaction,ref);
		BTree.Key k = h.key(ref.index);
		return k;
	}

	@Override
	public BTree.Key floor(BTree.Key key) {
		// start at root and find the correct leaf
		BTree.Key floor = null;
		long nOffset = transaction.root();
		int depth = transaction.depth();
		BTreeNode n = transaction.retrieve(nOffset);
		assert(n != null);
		for (int d = 0; d < depth; d++) {
			int x = n.find(key);
			if (x >= 0) return key; // the key exists in the tree, so return itself
			// key does not exist at this level

			BTreeIndexNode c = (BTreeIndexNode)n;
			x = -x-1; // find the item index where this key would be inserted
			if (x > 0) floor = c.key(x-1);
			nOffset = c.offset(x);
			if (nOffset == BTreeIndexNode.INVALID_OFFSET) return floor;

			n = transaction.retrieve(nOffset);
		}

		// check that we're on a leaf
		assert(n instanceof BTreeLeafNode);
		int x = n.find(key);
		if (x >= 0) return key;
		x = -x-1;
		if (x > 0) floor = n.key(x-1);

		return floor;
	}

	@Override
	public BTree.Key ceiling(BTree.Key key) {
		// start at root and find the correct leaf
		long nOffset = transaction.root();
		int depth = transaction.depth();
		BTreeNode n = transaction.retrieve(nOffset);
		assert(n != null);
		for (int d = 0; d < depth; d++) {
			int x = n.find(key);
			if (x >= 0) return key; // the key exists in the tree, so return itself
			// key does not exist at this level

			BTreeIndexNode c = (BTreeIndexNode)n;
			x = -x-1; // find the item index where this key would be inserted
			nOffset = c.offset(x);
			if (nOffset == BTreeIndexNode.INVALID_OFFSET) return null;
			n = transaction.retrieve(nOffset);
		}

		// check that we're on a leaf
		assert(n instanceof BTreeLeafNode);
		int x = n.find(key);
		if (x >= 0) return key;
		x = -x-1;

		BTree.Key k = n.key(x);

		return k;
	}

	@Override
	public Iterable<BTree.Reference> walk(BTree.Key fromKey, BTree.Key toKey) {
		return new RangeReferenceIterable(fromKey, toKey);
	}

	@Override
	public Iterable<BTree.Key> range(BTree.Key fromKey, BTree.Key toKey) {
		return new RangeKeyIterable(fromKey, toKey);
	}

	@Override
	public Iterable<BTree.Key> rangeOuter(BTree.Key fromKey, BTree.Key toKey) {
		return new OuterRangeKeyIterable(fromKey, toKey);
	}

	@Override
	//	@SuppressWarnings("unchecked")
	public boolean close(Iterator<?> i) {
		if (i != null && i instanceof RangeWalkerBase) {
			RangeWalkerBase r = (RangeWalkerBase)i;
			return r.close();
		}
		return false;
	}

	// -- convenience

	@Override
	public int fetch(BTree.Key key, long objectOffset, ByteBuffer buffer) {
		BTree.Reference ref = search(key);
		if (ref != null) return fetch(ref,objectOffset,buffer);
		else return 0;
	}

	// -- internals

	protected static BTreeLeafNode node(ReadOnlyBTreeBackingTransaction t, BTree.Reference ref) {
		if (ref == null) throw new NullPointerException("null reference");

		BTreeNode n = t.retrieve(ref.offset);
		if (n == null) throw new IllegalArgumentException("the reference offset does not refer to a valid node");

		if (!(n instanceof BTreeLeafNode)) throw new IllegalArgumentException("the reference is to an internal node");
		BTreeLeafNode nl = (BTreeLeafNode)n;

		return nl;
	}

	public static interface BTreeNodeVisitor {
		public void visit(int level, long offset, BTreeNode n);
	}

	public void visit(BTreeNodeVisitor visitor) {
		visit(visitor, transaction, 0, transaction.root());
	}

	private void visit(BTreeNodeVisitor visitor, ReadOnlyBTreeBackingTransaction t, int level, long offset) {
		BTreeNode n = t.retrieve(offset);
		assert (n != null) : String.format("level=%d offset=%d", level, offset);

		// pass to visitor
		visitor.visit(level, offset, n);

		// now visit the children
		if (n instanceof BTreeIndexNode) {
			BTreeIndexNode in = (BTreeIndexNode) n;
			for (int i = 0; i < n.children(); i++) {
				long child = in.offset(i);
				visit(visitor, t, level + 1, child);
			}
		}
	}

	public void print() {
		visit(new BTreeNodeVisitor() {
			@Override
			public void visit(int level, long offset, BTreeNode n) {
				System.out.printf(" l=%2d o=%6d ->", level, offset);
				for (int i = 0; i < n.children(); i++) {
					String r = "unknown";
					if (n instanceof BTreeLeafNode) {
						r = String.format("[k:%s s:%8x]", n.key(i), ((BTreeLeafNode) n).size(i));
					} else if (n instanceof BTreeIndexNode) {
						r = String.format("[k:%s c:%8d]", n.key(i), ((BTreeIndexNode) n).offset(i));
					}
					System.out.print(" {" + i + "}" + r);
				}
				System.out.println();
			}
		});
	}

	// -- iteration

	private class RangeKeyIterable implements Iterable<BTree.Key> {

		private final BTree.Key fromKey;
		private final BTree.Key toKey;

		public RangeKeyIterable(BTree.Key fromKey, BTree.Key toKey) {
			this.fromKey = fromKey;
			this.toKey = toKey;
		}

		@Override
		public Iterator<BTree.Key> iterator() {
			return new RangeKeyIterator(fromKey, toKey);
		}
	}

	private class OuterRangeKeyIterable implements Iterable<BTree.Key> {

		private final BTree.Key fromKey;
		private final BTree.Key toKey;

		public OuterRangeKeyIterable(BTree.Key fromKey, BTree.Key toKey) {
			this.fromKey = fromKey;
			this.toKey = toKey;
		}

		@Override
		public Iterator<BTree.Key> iterator() {
			return new OuterRangeKeyIterator(fromKey, toKey);
		}
	}

	private class RangeReferenceIterable implements Iterable<BTree.Reference> {

		private final BTree.Key fromKey;
		private final BTree.Key toKey;

		public RangeReferenceIterable(BTree.Key fromKey, BTree.Key toKey) {
			this.fromKey = fromKey;
			this.toKey = toKey;
		}

		@Override
		public Iterator<BTree.Reference> iterator() {
			return new RangeReferenceIterator(fromKey, toKey);
		}
	}

	private class RangeKeyIterator extends RangeWalker implements Iterator<BTree.Key> {

		public RangeKeyIterator(BTree.Key fromKey, BTree.Key toKey) {
			super(fromKey, toKey);
		}

		@Override
		public boolean hasNext() {
			return super.hasNext();
		}

		@Override
		public BTree.Key next() {
			return super.nextKey();
		}

		@Override
		public void remove() {
			super.remove();
		}

	}

	private class OuterRangeKeyIterator extends OuterRangeWalker implements Iterator<BTree.Key> {

		public OuterRangeKeyIterator(BTree.Key fromKey, BTree.Key toKey) {
			super(fromKey, toKey);
		}

		@Override
		public boolean hasNext() {
			return super.hasNext();
		}

		@Override
		public BTree.Key next() {
			return super.nextKey();
		}

		@Override
		public void remove() {
			super.remove();
		}

	}

	private class RangeReferenceIterator extends RangeWalker implements Iterator<BTree.Reference> {

		public RangeReferenceIterator(BTree.Key fromKey, BTree.Key toKey) {
			super(fromKey, toKey);
		}

		@Override
		public boolean hasNext() {
			return super.hasNext();
		}

		@Override
		public BTree.Reference next() {
			return super.nextReference();
		}

		@Override
		public void remove() {
			super.remove();
		}

	}

	private abstract class RangeWalkerBase {

		protected final BTree.Key fromKey;
		protected final BTree.Key toKey;

		protected final ReadOnlyBTreeBackingTransaction transaction;

		protected final BTree.Reference nextRef;
		protected BTree.Key next;

		protected final Deque<BTree.Reference> path;
		protected final int depth;

		private boolean skipCeiling;

		public RangeWalkerBase(BTree.Key fromKey, BTree.Key toKey, boolean skipCeiling) {
			this.transaction = ReadOnlyTransactionBTree.this.transaction;

			this.fromKey = fromKey;
			this.toKey = toKey;

			this.nextRef = new BTree.Reference();
			this.next = null;

			this.path = new ArrayDeque<BTree.Reference>();
			this.depth = transaction.depth();

			this.skipCeiling = skipCeiling;
		}

		protected boolean close() {
			//			if (next != null || (transaction != null && transaction.isOpen())) {
			//				if (transaction != null) transaction.close();
			//				next = null;
			//				return true;
			//			}
			//			return false;
			return true;
		}

		public boolean hasNext() {
			if (next == null) {
				return false;
			} else {
				return true;
			}
		}

		public BTree.Key nextKey() {
			BTree.Key n = next;
			step();
			return n;
		}

		public BTree.Reference nextReference() {
			if (next == null) return null;
			BTree.Reference r = new BTree.Reference(nextRef);
			step();
			return r;
		}

		public void remove() {
			throw new UnsupportedOperationException("BTree interators to not support element removal");
		}

		@Override
		protected void finalize() {
			// double check that we are releasing
			// the underlying transaction
			close();
		}

		protected final void step() {
			try {

				walk:
					while(true) {

						if (path.isEmpty()) {
							next = null;
							break walk;
						}

						BTree.Reference l = path.peekLast();
						BTreeNode n = transaction.retrieve(l.offset);

						if (path.size() == depth + 1) {
							// traverse the leaf
							if (l.index < n.children()) {
								BTree.Key k = n.key(l.index);
								nextRef.copy(l);
								l.index++;

								// Note, we could optimise be performing less comparisons by leveraging layout
								// of keys in nodes and ignoring per item check if the rightmost item in the
								// a given leaf node is smaller than 'toKey'.
								int c = k.compareTo(toKey);
								if (c > 0 && skipCeiling) {
									next = null;
									path.clear();
									break walk;
								}
								if (c >= 0) skipCeiling = true;
								next = k;
								break walk;
							} else {
								path.removeLast();
								if (!path.isEmpty()) path.peekLast().index++;
								continue walk;
							}
						} else {
							// need to move across tree
							if (l.index < n.children()) {
								BTreeIndexNode c = (BTreeIndexNode)n;
								long nOffset = c.offset(l.index);
								BTree.Reference r = new BTree.Reference(nOffset, 0, 0);
								path.addLast(r);
								continue walk;
							} else {
								path.removeLast();
								if (!path.isEmpty()) path.peekLast().index++;
								continue walk;
							}

						}
					}

			} finally {
				// close the transaction as soon as possible
				// (review, but this looks like it was carried over from earlier era... and should not be here)
				//				if (next == null && transaction.isOpen()) transaction.close();
			}
		}
	}

	private abstract class RangeWalker extends RangeWalkerBase {

		public RangeWalker(BTree.Key fromKey, BTree.Key toKey) {
			super(fromKey,toKey, true);
			init();
			step();
		}

		private void init() {
			// start at root and find the leaf containing the ceiling of the 'fromKey'
			long nOffset = transaction.root();
			BTreeNode n = transaction.retrieve(nOffset);
			assert(n != null);
			for (int d = 0; d < depth; d++) {
				int x = n.find(fromKey);
				if (x < 0) x = -x-1;
				BTreeIndexNode c = (BTreeIndexNode)n;
				path.addLast(new BTree.Reference(nOffset, x, 0));
				nOffset = c.offset(x);
				if (nOffset == BTreeIndexNode.INVALID_OFFSET) {
					// ceiling of start not found so no keys to show
					path.clear();
					return;
				}
				n = transaction.retrieve(nOffset);
			}

			// check that we're on a leaf
			assert(n instanceof BTreeLeafNode);
			int x = n.find(fromKey);
			// if the exact key is not in the node, we simply pick the next one along
			if (x < 0) x = -x-1;
			path.addLast(new BTree.Reference(nOffset, x, 0));

		}

	}

	private abstract class OuterRangeWalker extends RangeWalkerBase {

		public OuterRangeWalker(BTree.Key fromKey, BTree.Key toKey) {
			super(fromKey, toKey, false);

			init();
			step();
		}

		private void init() {
			// start at root and find the leaf containing the floor of the 'fromKey'

			long nOffset = transaction.root();

			int floorDepth = -1;
			BTree.Reference floor = new BTree.Reference(nOffset,-1,0);

			BTreeNode n = transaction.retrieve(nOffset);
			assert(n != null);
			int d;
			for (d = 0; d < depth; d++) {
				int x = n.find(fromKey);
				if (x < 0) {
					// key does not exist here
					// so find insertion point
					x = -x-1;
					// and keep track of the best floor to date
					if (x > 0) {
						floor.index = x-1;
						floor.offset = nOffset;
						floorDepth = d;
					}
				} else {
					floor.index = x;
					floor.offset = nOffset;
					floorDepth = d;
				}

				path.addLast(new BTree.Reference(nOffset, x, 0));

				BTreeIndexNode c = (BTreeIndexNode)n;
				nOffset = c.offset(x);
				if (nOffset == BTreeIndexNode.INVALID_OFFSET) {
					// run out of options in this direction
					break;
				}
				n = transaction.retrieve(nOffset);
			}

			if (d == depth) {
				// check that we're on a leaf
				assert(n instanceof BTreeLeafNode);
				int x = n.find(fromKey);
				// if the exact key is not in the node, we simply pick the next one along
				if (x >= 0) {
					//					floor.index = x;
					//					floor.offset = nOffset;
					//					floorDepth = depth;
					path.addLast(new BTree.Reference(nOffset, x, 0));
					return;
				} else {
					x = -x-1;
					if (x > 0) {
						//						floor.index = x-1;
						//						floor.offset = nOffset;
						//						floorDepth = depth;
						path.addLast(new BTree.Reference(nOffset, x-1, 0));
						return;
					}
				}
			}

			// hmmm, edge case so we need to backtrack
			if (floorDepth >= 0) {
				// ok, lets go back to the last good floor and walk down the right children from there
				while(path.size() > floorDepth) path.removeLast();
				assert(path.size() == floorDepth);

				path.addLast(floor);

				n = transaction.retrieve(floor.offset);
				BTreeIndexNode c = (BTreeIndexNode)n;
				nOffset = c.offset(floor.index);
				assert(nOffset > BTreeIndexNode.INVALID_OFFSET);
				n = transaction.retrieve(nOffset);
				for (d = floorDepth+1; d < depth; d++) {
					int x = n.children();
					if (x <= 0) break;
					path.addLast(new BTree.Reference(nOffset, x-1, 0));
					assert(n instanceof BTreeIndexNode);
					c = (BTreeIndexNode)n;
					nOffset = c.offset(x-1);
					n = transaction.retrieve(nOffset);
				}
				if (d == depth) {
					assert(n instanceof BTreeLeafNode);
					int x = n.children();
					if (x <= 0) path.clear();
					else path.addLast(new BTree.Reference(nOffset, x-1, 0));
				} else {
					path.clear();
				}
			} else {
				// ah, there is no floor, therefore we must simply start at the lowest entry in the tree
				path.clear();
				nOffset = transaction.root();
				for (d = 0; d < depth; d++) {
					path.addLast(new BTree.Reference(nOffset, 0, 0));
					n = transaction.retrieve(nOffset);
					BTreeIndexNode c = (BTreeIndexNode)n;
					nOffset = c.offset(0);
					if (nOffset == BTreeIndexNode.INVALID_OFFSET) break;
				}
				if (d == depth) {
					path.addLast(new BTree.Reference(nOffset, 0, 0));
				} else {
					path.clear(); // nothing to look at :( tree must be empty
				}
			}

		}

	}

}
