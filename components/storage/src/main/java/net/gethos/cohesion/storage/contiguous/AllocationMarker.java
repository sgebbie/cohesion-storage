/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.contiguous;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTransaction;
import net.gethos.cohesion.storage.ReadOnlyBTreeTransaction;
import net.gethos.cohesion.storage.ReadOnlyTransactionBTree;
import net.gethos.cohesion.storage.TransactionBTree;
import net.gethos.cohesion.storage.BTree.Key;
import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeIndexNode;
import net.gethos.cohesion.storage.backing.BTreeLeafNode;
import net.gethos.cohesion.storage.buffer.BufferSuperNode;
import net.gethos.cohesion.storage.buffer.NodeCapacities;
import net.gethos.cohesion.storage.store.ContiguousStore;

/**
 * Store allocation markers in the B-Tree.
 * <p>
 * <ul>
 *   <li>key.idHigh = region start offset</li>
 *   <li>key.type = allocated or free (or unknown)</li>
 * </ul>
 * 
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public /* TODO remove public */ enum AllocationMarker {

	UNKOWN(0x0)
	, MIN(Short.MIN_VALUE)
	, MAX(Short.MAX_VALUE)
	, ALLOCATED(0x1)
	, FREE(0x2)
	;

	/**
	 * The lowest index, so that the allocation tree is kept close to the
	 * beginning of the tree.
	 */
	static final short ALLOCATION_IDX = Short.MIN_VALUE;

	/**
	 * The allocation markers do not actually hold any associated data.
	 */
	static final int ALLOCATION_DATA_SIZE = 0;

	public static final Key MAX_ALLOCATION_KEY;
	public static final Key MIN_ALLOCATION_KEY;

	static {
		MAX_ALLOCATION_KEY = new Key();
		MAX_ALLOCATION_KEY.idx       = ALLOCATION_IDX;
		MAX_ALLOCATION_KEY.idHigh    = (long) 0xffffffffffffffffL;
		MAX_ALLOCATION_KEY.idMiddle  = (long) 0xffffffffffffffffL;
		MAX_ALLOCATION_KEY.idLow     = (int)  0xffffffff;
		MAX_ALLOCATION_KEY.type      = MAX.code;
		MAX_ALLOCATION_KEY.parameter = 0;

		MIN_ALLOCATION_KEY = new Key();
		MIN_ALLOCATION_KEY.idx       = ALLOCATION_IDX;
		MIN_ALLOCATION_KEY.idHigh    = (long) 0x0L;
		MIN_ALLOCATION_KEY.idMiddle  = (long) 0x0L;
		MIN_ALLOCATION_KEY.idLow     = (int)  0x0;
		MIN_ALLOCATION_KEY.type      = MIN.code;
		MIN_ALLOCATION_KEY.parameter = 0;
	}

	public final short code;

	AllocationMarker(int code) {
		this.code = (short) code;
	}

	public BTree.Key key(long offset) {
		BTree.Key k = new BTree.Key();
		k.idx = ALLOCATION_IDX;
		k.idHigh = offset;
		k.idMiddle = 0;
		k.idLow = 0;
		k.type = code;
		return k;
	}

	public static long offset(BTree.Key key) {
		return key.idHigh;
	}

	public boolean matches(BTree.Key key) {
		if (key == null) return false;
		return (key.idx == ALLOCATION_IDX && key.type == code);
	}

	public static boolean isMarker(BTree.Key key) {
		if (key == null) return false;
		return key.idx == ALLOCATION_IDX;
	}

	public static String toString(BTree.Key key) {
		if (!isMarker(key)) return "(bad-marker)";
		return String.format("[%s:@%05d]",AllocationMarker.flag(key),AllocationMarker.offset(key));
	}

	public static String flag(Key k) {
		if (k.type == AllocationMarker.ALLOCATED.code) return "A";
		if (k.type == AllocationMarker.FREE.code) return "F";
		return "U";
	}

	// -- allocation

	static boolean allocRange(BTreeTransaction allocationTree, long offset, long length) {
		return reassignAllocation(allocationTree, offset, length, AllocationMarker.ALLOCATED, AllocationMarker.FREE);
	}

	static boolean freeRange(BTreeTransaction allocationTree, long offset, long length) {
		return reassignAllocation(allocationTree, offset, length, AllocationMarker.FREE, AllocationMarker.ALLOCATED);
	}

	static Range findTail(ReadOnlyTransactionBTree bt) {
		BTree.Key tail = bt.floor(AllocationMarker.MAX_ALLOCATION_KEY);
		return new Range(AllocationMarker.offset(tail),Long.MAX_VALUE);
	}

	/**
	 * Find a suitable free range in the allocation tree.
	 * 
	 * @param bt - transaction for accessing the allocation tree
	 * @param offset - the lowest offset which will be considered during the search
	 * @param length - the space required
	 * @return The key marking the start of the suitable free range, or null if
	 *         no range was found.
	 */
	static Range findFree(ReadOnlyTransactionBTree bt, long offset, long length) {

		//		System.out.printf("AllocationMarker.findFree(,%d,%d) - alloc tree: %s%n", offset, length, AllocationMarker.toString(bt));

		// Note, as an invariant, the AllocationTree is assumed
		//       to always start with an 'allocated' marker, and
		//       to always end with a 'free' marker.

		Range actx = new Range();

		BTree.Key f = null;
		BTree.Key a = null;

		// start searching the allocation tree at the given offset
		Iterator<BTree.Key> i = null;
		try {
			i = bt.range(AllocationMarker.MIN.key(offset), AllocationMarker.MAX_ALLOCATION_KEY).iterator();

			scan:
				while (i.hasNext()) {
					// find a free marker
					f = null;
					for (; i.hasNext();) {
						BTree.Key m = i.next();
						if (m.idx == AllocationMarker.ALLOCATION_IDX && m.type == AllocationMarker.FREE.code) {
							f = m;
							break;
						}
					}
					if (f == null) break scan; // ah, bugger :(

					// find the next allocated marker
					a = null;
					for (; i.hasNext();) {
						BTree.Key m = i.next();
						if (m.idx == AllocationMarker.ALLOCATION_IDX && m.type == AllocationMarker.ALLOCATED.code) {
							a = m;
							break;
						}
					}
					if (a == null) break scan; // Wahooo! the free range was unbounded... allocate as much as you want ;)

					// check the length
					long l = a.idHigh - f.idHigh;
					if (l >= length) {
						actx.offset = AllocationMarker.offset(f);
						actx.length = l;
						return actx; // OK, good to go, just don't get carried away.
					}
				}
		} finally {
			bt.close(i);
		}

		if (f == null || a == null) {
			actx.offset = f == null ? offset : AllocationMarker.offset(f);
			actx.length = Long.MAX_VALUE;
			return actx; // Well let's just assume it all free then...
		}

		return null;
	}

	private static boolean reassignAllocation(BTreeTransaction allocationTree, long offset, long length, AllocationMarker newMarker, AllocationMarker otherMarker) {

		// Note, the allocation tree should always have at least
		// two entries. The first entry should always be an 'allocated' marker
		// and the last entry should always be a 'free' marker.
		// A new allocation tree has [allocated:@0] followed by [free:@0]

		if (length <= 0) return false;

		Deque<BTree.Key> additions = new ArrayDeque<BTree.Key>();
		Deque<BTree.Key> deletions = new ArrayDeque<BTree.Key>();

		// consider the region from [offset,offset+length)
		// find the floor and ceiling of the region
		Iterator<BTree.Key> i = null;
		try {
			long end = offset+length;
			BTree.Key startk = AllocationMarker.MIN.key(offset);
			BTree.Key startkm = AllocationMarker.MAX.key(offset);
			BTree.Key endk = AllocationMarker.MAX.key(end);
			i = allocationTree.rangeOuter(startk, endk).iterator();
			BTree.Key floorStart = null;
			BTree.Key floorEnd = null;
			for (;i.hasNext();) {
				BTree.Key k = i.next();
				if (!AllocationMarker.isMarker(k)) continue;
				if (k.compareTo(startkm) > 0) break;
				floorStart = k;
				break;
			}
			if (floorStart == null) {
				return false; // should not happen as there should always be a 0 key in the tree
			}

			floorEnd = floorStart;

			// pick out all the inner markers
			for (;i.hasNext();) {
				BTree.Key k = i.next();
				if (!AllocationMarker.isMarker(k)) continue;
				if (k.compareTo(endk) < 0) {
					floorEnd = k;
					deletions.add(k);
				}
			}

			// now decide on the edge cases
			if (otherMarker.matches(floorStart)) {
				additions.add(newMarker.key(offset));
			}
			if (otherMarker.matches(floorEnd)) {
				additions.add(otherMarker.key(end));
			}

		} finally {
			allocationTree.close(i);
		}

		// make changes to the tree
		//		System.out.printf("deletions=%s additions=%s%n",deletions,additions);
		//		for (BTree.Key k : deletions) allocationTree.delete(k);
		//		for (BTree.Key k : additions) allocationTree.truncate(k, 0);

		boolean replaced = false;
		if (deletions.size() == 1 && additions.size() == 1 && !additions.peekFirst().equals(deletions.peekFirst())) {
			// use key replace - this means that the change is done in place without causing a structural
			//                   change to the tree. Therefore there are no spurious node deletes and
			//                   inserts. Combined with the switch to tail allocation during the commit
			//                   phase, this helps to ensure that the process completed.
			replaced = ((TransactionBTree)allocationTree).replace(deletions.peekFirst(),additions.peekFirst());
		}
		if (!replaced) {
			for (BTree.Key k : additions) {
				if (deletions.contains(k)) continue; // clearly both and add and delete, so ignore
				allocationTree.truncate(k, AllocationMarker.ALLOCATION_DATA_SIZE);
			}
			for (BTree.Key k : deletions) {
				if (additions.contains(k)) continue; // clearly both and add and delete, so ignore
				allocationTree.delete(k);
			}
		}

		return true;
	}

	// -- bootstrap

	/**
	 * Initialises an allocation tree with two elements: [A:0][F:0].
	 */
	static void bootstrap(BTreeBacking backing, long allocateStart, long freeStart) {
		BTreeBackingTransaction t = null;
		try {
			t = backing.open();
			bootstrap(t, allocateStart, freeStart);
			t.commit();
		} finally {
			if (t != null) t.close();
		}
	}

	/**
	 * Initialises an contiguous store with an allocation tree with two elements: [A:0][F:0].
	 */
	static void bootstrap(BTreeBackingTransaction t, long allocateStart, long freeStart) {
		long root = t.alloc(false);
		long leaf = t.alloc(true);
		BTreeIndexNode r = (BTreeIndexNode)t.retrieve(root);
		BTreeLeafNode l = (BTreeLeafNode)t.retrieve(leaf);
		BTree.Key a0 = AllocationMarker.ALLOCATED.key(allocateStart);
		BTree.Key f0 = AllocationMarker.FREE.key(freeStart);
		r.writeRight(leaf);
		l.realloc(a0, ALLOCATION_DATA_SIZE);
		l.realloc(f0, ALLOCATION_DATA_SIZE);

		t.recordRoot(1, root);
		t.record(root, r);
		t.record(leaf, l);
	}

	/**
	 * Initialises an contiguous store with an allocation tree with two elements: [A:0][F:0].
	 */
	static void bootstrap(ContiguousStore store, NodeCapacities nodeCapacities) {

		int blockSize = ReadOnlyContiguousBackingTransaction.NODE_SIZE;

		// bootstrap
		BTreeBackingTransaction t = null;
		try {
			long[] allocationPoints = new long[10];
			for (int i = 0; i < allocationPoints.length; i++) allocationPoints[i] = blockSize*(i+1); // NB skip allocation of the header
			t = new TrivialContiguousTransaction(store, nodeCapacities, allocationPoints);
			AllocationMarker.bootstrap(t, 0, BufferSuperNode.SUPER_NODE_SIZE+blockSize*2); // two nodes and the header
			//			System.out.println(AllocationMarker.toString(new ReadOnlyTransactionBTree(t)));
			t.commit();
		} finally {
			if (t != null) t.close();
		}

	}

	// -- dump

	/**
	 * Dump the allocation tree markers for diagnostics.
	 * 
	 * @param allocationTree
	 * @return a dump of the allocation tree markers
	 */
	public static String toString(ReadOnlyBTreeTransaction allocationTree) {
		StringBuilder s = new StringBuilder();
		Iterator<BTree.Key> i = null;
		try {
			Iterable<BTree.Key> range = allocationTree.range(AllocationMarker.MIN.key(0), AllocationMarker.MAX_ALLOCATION_KEY);
			i = range.iterator();
			for(;i.hasNext();) {
				BTree.Key ak = i.next();
				s.append(ak.idHigh).append(' ').append(AllocationMarker.flag(ak)).append(' ');
			}
		} finally {
			if (i != null) allocationTree.close(i);
		}

		return s.toString();
	}
}
