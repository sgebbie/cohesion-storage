/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import net.gethos.cohesion.common.UnsignedUtils;
import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.buffer.HeapBufferBacking;
import net.gethos.cohesion.storage.heap.HeapCloneBacking;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
@RunWith(Parameterized.class)
public class BTreeIteratorTest extends BTreeTestBase {
	
	private final int testSize;
	private final Class<? extends BTreeBacking> backingClass;
	private final int expectDepth;
	
	public BTreeIteratorTest(Class<? extends BTreeBacking> backingClass, int size, int expectDepth) {
		this.backingClass = backingClass;
		this.testSize = size;
		this.expectDepth = expectDepth;
	}

	@Parameters
	public static Collection<Object[]> parameters() {
		ArrayList<Object[]> runs = new ArrayList<Object[]>();
		
		runs.add(new Object[]{HeapCloneBacking.class,100,1});
		runs.add(new Object[]{HeapCloneBacking.class,35000,2});
		runs.add(new Object[]{HeapBufferBacking.class,100,1});
		runs.add(new Object[]{HeapBufferBacking.class,35000,2});
		
		return runs;
	}

	
	private static final int TEST_CAPACITY = 200;

	private BTree btree;
	private byte[] data;
	
	private  SortedSet<Long> keyValues;
	private List<Long> shuffled;
	
	private Random random;
	
	@Override
	protected Random random() {
		return random;
	}
	
	@Before
	public void setUp() {
		
		this.random = new Random();

		data = random(100);
		
		btree = newBTreeInstance(backingClass, TEST_CAPACITY);
		
		// create some random keys
		keyValues = new TreeSet<Long>(new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				return UnsignedUtils.compareUnsigned(o1,o2);
			}
		});
		
		while (keyValues.size() < testSize) {
			long k = random.nextLong();
			
			// make the number even
			// (this way we can always add 1 or subtract one to get a bigger or smaller number that is not in the list)
			k = 2*(k/2);
			assert(k % 2 == 0);
			
			// remove some edge cases
			if (k == 0) k = 1;
			if (k == Long.MAX_VALUE) k = Long.MAX_VALUE - 1;
			
			keyValues.add(k);
		}
		
		// create a shuffled list of the unique keys
		shuffled = new ArrayList<Long>(keyValues);
		Collections.shuffle(shuffled,random);
		
		// store some data using the shuffled list rather than the sorted list
		ByteBuffer datab = ByteBuffer.wrap(data);
		BTreeTransaction bt = btree.open();
		for (long k : shuffled) {
			BTree.Key key = createKey(k);
			datab.clear();
			BTree.Reference r = bt.store(key, 0, datab);
			assertNotNull(r);
		}
		bt.commit();
		bt.close();
		assertEquals("Btree depth not great enough to exercise all cases: ",expectDepth,btree.depth());
	}
	
	@Test
	public void floor() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		for (long k : keyValues) {
			BTree.Key expect = createKey(k);
			// create a key slightly larger than the one in the tree
			BTree.Key larger = createKey(k);
			larger.idLow = 1;	
			assertTrue(larger.compareTo(expect) > 0);
			
			// find floor
//			System.out.println("checking k=" + k + " expect=" + expect + " larger=" + larger);
			BTree.Key f = bt.floor(larger);
			assertEquals(expect, f);
		}
		bt.close();
	}
	
	@Test
	public void floorDown() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		for (long k : keyValues) {
			SortedSet<Long> h = keyValues.headSet(k);
			Long kdown = h.isEmpty() ? null : h.last();
			BTree.Key expect = kdown == null ? null : createKey(kdown);
			// create a key slightly smaller than the one in the tree
			BTree.Key smaller = createKey(k);
			smaller.type -= 1;
			assertTrue(expect == null || smaller.compareTo(expect) > 0);
			
			// find floor
//			System.out.println("checking k=" + k + " expect=" + expect + " larger=" + larger);
			BTree.Key f = bt.floor(smaller);
			assertEquals(expect, f);
		}
		bt.close();
	}
	
	@Test
	public void floorEdge() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		long k = keyValues.first();
		assertTrue(k != 0);
		BTree.Key expect = createKey(k);
		// create a key slightly smaller than the smallest in the tree
		BTree.Key smaller = createKey(k-1);
		assertTrue(smaller.compareTo(expect) < 0);
			
		// find floor
//		System.out.println("checking k=" + k + " expect=" + expect + " smaller=" + smaller);
		BTree.Key f = bt.floor(smaller);
		assertNull(f);
		bt.close();
	}
	
	@Test
	public void ceiling() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		for (long k : keyValues) {
			BTree.Key expect = createKey(k);
			// create a key slightly smaller than the one in the tree
			BTree.Key smaller = createKey(k-1);
			smaller.idMiddle = Long.MAX_VALUE;
			smaller.idLow = Integer.MAX_VALUE;
			assertTrue(smaller.compareTo(expect) < 0);
			
			// find floor
//			System.out.println("checking k=" + k + " expect=" + expect + " smaller=" + smaller);
			BTree.Key f = bt.ceiling(smaller);
			assertEquals(expect, f);
		}
		bt.close();
	}
	
	@Test
	public void ceilingEdge() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		long k = keyValues.last();
		assertTrue(k != Long.MAX_VALUE);
		BTree.Key expect = createKey(k);
		// create a key slightly larger than the largest in the tree
		BTree.Key larger = createKey(k);
		larger.idLow = 1;
		assertTrue(larger.compareTo(expect) > 0);
			
		// find floor
//		System.out.println("checking k=" + k + " expect=" + expect + " larger=" + larger);
		BTree.Key f = bt.ceiling(larger);
		assertNull(f);
		bt.close();
	}
	
	@Test
	public void ceilingMin() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		// ceiling(MIN_KEY) is equivalent to finding the smallest key in the tree
		
		long k = keyValues.first();
		assertTrue(k != 0);
		BTree.Key expect = createKey(k);
		BTree.Key zero = BTree.Key.MIN_KEY;
		assertTrue(zero.compareTo(expect) < 0);
			
		// find floor
//		System.out.println("checking k=" + k + " expect=" + expect + " other=" + zero);
		BTree.Key f = bt.ceiling(zero);
		assertNotNull(f);
		assertEquals(expect,f);
		bt.close();
	}
	
	@Test
	public void floorMax() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		// floor(MAX_KEY) is equivalent to finding the largest key in the tree
		
		long k = keyValues.last();
		assertTrue(k != Long.MAX_VALUE);
		BTree.Key expect = createKey(k);
		// create a key slightly larger than the largest in the tree
		BTree.Key max = BTree.Key.MAX_KEY;
		assertTrue(max.compareTo(expect) > 0);
			
		// find floor
//		System.out.println("checking k=" + k + " expect=" + expect + " other=" + max);
		BTree.Key f = bt.floor(max);
		assertNotNull(f);
		assertEquals(expect,f);
		bt.close();
	}
	
	@Test
	public void walkKeys() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		List<Long> rangeValues = pickRange();
		
		// now walk the btree and collect values
		List<Long> collectedValues = new ArrayList<Long>();
		long lowValue = rangeValues.get(0) - 1;
		long highValue = rangeValues.get(rangeValues.size()-1) + 1;
		Iterable<BTree.Key> range = bt.range(createKey(lowValue), createKey(highValue));
		assertNotNull(range);
		for (BTree.Key key : range)	collectedValues.add(key.idHigh);
		
		Collection<Long> collectedUnique = new TreeSet<Long>(collectedValues);
		assertEquals(collectedUnique.size(),collectedValues.size());
		
		// check for missing
		Collection<Long> missing = new TreeSet<Long>(rangeValues);
		missing.removeAll(collectedValues);
		assertTrue(missing.isEmpty());
//		System.out.printf(" inserted keys count=%d walk range count=%d collected key count=%d collected uniq count=%d missing key count=%d%n", keyValues.size(), rangeValues.size(), collectedValues.size(), collectedUnique.size(), missing.size());
//		for (Long k : missing) {
//			System.out.printf(" %x", k);
//		}
		
//		boolean savemissing = true;
//		if (savemissing) {
//			File keyFile = new File("/home/stewart/work/gethos/Cohesion/btree-keys.txt");
//			try {
//				PrintStream s = new PrintStream(keyFile);
//				try {
//					for (long k : rangeValues) {
//						s.printf("i %016x%n", k);
//					}
//					for (long k : missing) {
//						s.printf("m %016x%n", k);
//					}
//				} finally {
//					s.close();	
//				}
//			} catch (IOException e) {
//				throw new AssertionError(e);
//			}
//		}
		
		// check that the range produced by the btree matches the expected range
		assertEquals(rangeValues.size(), collectedValues.size());
		assertEquals(rangeValues, collectedValues);
		bt.close();
	}
	
	@Test
	public void walkKeysNoFloorOrCeiling() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		// now walk the btree and collect values
		List<Long> collectedValues = new ArrayList<Long>();
		long lowValue = keyValues.first() - 1;
		long highValue = keyValues.last() + 1;
		Iterable<BTree.Key> range = bt.range(createKey(lowValue), createKey(highValue));
		assertNotNull(range);
		for (BTree.Key key : range)	collectedValues.add(key.idHigh);
		
		// check that the range produced by the btree matches the expected range
		assertEquals(keyValues.size(), collectedValues.size());
		assertTrue(equals(keyValues,collectedValues));
		bt.close();
	}
	
	@Test
	public void walkKeysOuterNoFloorOrCeiling() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		// now walk the btree and collect values
		List<Long> collectedValues = new ArrayList<Long>();
		long lowValue = keyValues.first() - 1;
		long highValue = keyValues.last() + 1;
		Iterable<BTree.Key> range = bt.rangeOuter(createKey(lowValue), createKey(highValue));
		assertNotNull(range);
		for (BTree.Key key : range)	collectedValues.add(key.idHigh);
		
		// check that the range produced by the btree matches the expected range
		assertEquals(keyValues.size(), collectedValues.size());
		assertTrue(equals(keyValues,collectedValues));
		bt.close();
	}
	
	@Test
	public void walkOuterKeys() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		List<Long> rangeValues = pickRange();
		
		// now walk the btree and collect values
		List<Long> collectedValues = new ArrayList<Long>();
		long lowValue = rangeValues.get(0) + 1;
		long highValue = rangeValues.get(rangeValues.size()-1) - 1;
		Iterable<BTree.Key> range = bt.rangeOuter(createKey(lowValue), createKey(highValue));
		assertNotNull(range);
		// walk the range
		for (BTree.Key key : range)	collectedValues.add(key.idHigh);
		
		// check that the range produced by the btree matches the expected range
		assertEquals(rangeValues.size(), collectedValues.size());
		assertEquals(rangeValues, collectedValues);
		bt.close();
	}
	
	@Test
	public void walkReferences() {
		ReadOnlyBTreeTransaction bt = btree.openReadOnly();
		List<Long> rangeValues = pickRange();
		
		// now walk the btree and collect values
		List<Long> collectedValues = new ArrayList<Long>();
		long lowValue = rangeValues.get(0);
		long highValue = rangeValues.get(rangeValues.size()-1);
		Iterable<BTree.Reference> range = bt.walk(createKey(lowValue), createKey(highValue));
		assertNotNull(range);
		for (BTree.Reference r : range) {
			BTree.Key key = bt.key(r);
			collectedValues.add(key.idHigh);
		}
		
		// check that the range produced by the btree matches the expected range
		assertEquals(rangeValues.size(), collectedValues.size());
		assertEquals(rangeValues, collectedValues);
		bt.close();
	}
	
	@Test
	public void walkKeysEmpty() {
		BTree bt = newBTreeInstance(backingClass, TEST_CAPACITY);
		List<Long> collectedValues = new ArrayList<Long>();
		Iterable<BTree.Key> range = bt.openReadOnly().range(createKey(1), createKey(100));
		assertNotNull(range);
		for (BTree.Key key : range) collectedValues.add(key.idHigh);
		assertTrue(collectedValues.isEmpty());
	}
	
	@Test
	public void walkOuterKeysEmpty() {
		BTree bt = newBTreeInstance(backingClass, TEST_CAPACITY);
		List<Long> collectedValues = new ArrayList<Long>();
		Iterable<BTree.Key> range = bt.openReadOnly().rangeOuter(createKey(1), createKey(100));
		assertNotNull(range);
		for (BTree.Key key : range) collectedValues.add(key.idHigh);
		assertTrue(collectedValues.isEmpty());
	}

	@Test
	public void walkShort() {
		int from = 5;
		int to = 25;
		BTree.Key fromKey = createKey(from);
		BTree.Key toKey = createKey(to);
		walkShort(0,30,5,25,fromKey,toKey);
	}
	
	@Test
	public void walkZero() {
		walkShort(1,1,1,0,BTree.Key.MIN_KEY,BTree.Key.MAX_KEY);
	}
	
	@Test
	public void walkOne() {
		walkShort(1,2,1,1,BTree.Key.MIN_KEY,BTree.Key.MAX_KEY);
	}
	
	@Test
	public void walkAll() {
		walkShort(0, 30,0,29,BTree.Key.MIN_KEY,BTree.Key.MAX_KEY);
	}
	
	private void walkShort(int minCreate, int maxCreate, int checkFrom, int checkTo, BTree.Key fromKey, BTree.Key toKey) {
		BTree bt = newBTreeInstance(HeapCloneBacking.class, 10);

		ByteBuffer datab = ByteBuffer.wrap(data);
		BTreeTransaction btt = bt.open();
		for (long k = minCreate; k < maxCreate; k++) {
			BTree.Key key = createKey(k);
			datab.clear();
			BTree.Reference r = btt.store(key, 0, datab);
			assertNotNull(r);
		}
		btt.commit();
		btt.close();
		
		Iterable<BTree.Key> range = bt.openReadOnly().range(fromKey, toKey);
		assertNotNull(range);
		List<Long> collected = new ArrayList<Long>();
		for (BTree.Key key : range) {
			collected.add(key.idHigh);
//			System.out.println("got key = " + key);
		}
		for (long k = checkFrom; k <= checkTo; k++) {
			assertTrue(collected.contains(k));
		}
		assertEquals(checkTo - checkFrom + 1, collected.size());
	}
	
	private List<Long> pickRange() {
		
		// pick the low 10% mark and try walk to the high 90%
		int low = (int)(keyValues.size() * 0.1);
		int hi = (int)(keyValues.size() * 0.9);
		
		long lowValue = 0;
//		long highValue = 0;
		long highNextValue = 0;
		int p = 0;
		for (long k : keyValues) {
			if (p++ == low) lowValue = k;
//			if (p == hi) highValue = k;
			if (p == hi+1) highNextValue = k;
		}
		
		// pull out the range set [lowValue,highValue]
		SortedSet<Long> rangeSet = keyValues.subSet(lowValue,highNextValue);
		List<Long> rangeValues = new ArrayList<Long>(rangeSet);
		
		assertFalse(rangeValues.isEmpty());
		
		return rangeValues;
	}
	
	/**
	 * 
	 * @param <T>
	 * @param a
	 * @param b
	 * @return true if two collections are equal, element for element.
	 */
	private static <T> boolean equals(Collection<? extends T> a, Collection<? extends T> b) {
		if (a == b) return true;
		if (a.size() != b.size()) return false;
		Iterator<? extends T> ai = a.iterator();
		Iterator<? extends T> bi = b.iterator();
		while(true) {
			if (!ai.hasNext()) break;
			if (!bi.hasNext()) break;
			T ax = ai.next();
			T bx = bi.next();
			if (ax == bx) continue;
			if (ax.equals(bx)) continue;
			return false;
		}
		if (ai.hasNext()) return false;
		if (bi.hasNext()) return false;
		return true;
	}
}
