/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import net.gethos.cohesion.storage.backing.BTreeBacking;
import net.gethos.cohesion.storage.buffer.HeapBufferBacking;
import net.gethos.cohesion.storage.contiguous.WinnowingContiguousByteBufferBacking;
import net.gethos.cohesion.storage.heap.HeapBacking;
import net.gethos.cohesion.storage.heap.HeapCloneBacking;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
@RunWith(Parameterized.class)
public class BTreeTest extends BTreeTestBase {

	private static final int TEST_CAPACITY = 5;
	private BTree btree;
	
	private final Class<? extends BTreeBacking> backingClass;
	
	public BTreeTest(Class<? extends BTreeBacking> backingClass) {
		this.backingClass = backingClass;
	}
	
	@Parameters
	public static Collection<Object[]> parameters() {
		ArrayList<Object[]> runs = new ArrayList<Object[]>();
		
		runs.add(new Object[]{HeapBacking.class});
		runs.add(new Object[]{HeapCloneBacking.class});
		runs.add(new Object[]{HeapBufferBacking.class});
		runs.add(new Object[]{WinnowingContiguousByteBufferBacking.class});	
		
		return runs;
	}
	
	private Random random;
	
	@Override
	protected Random random() {
		return random;
	}
	
	@Before
	public void setUp() throws SecurityException, IllegalArgumentException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
		this.random = new Random(123);
		this.btree = newBTreeInstance(backingClass, TEST_CAPACITY);
	}
	
	@Test
	public void deleteDepthCollapse() {
		buildAndDelete(btree,50,true);
		assertEquals(1,btree.depth());
	}
	
	@Test
	public void deleteDepthCollapseDifferentOrder() {
		buildAndDelete(btree,50,false);
	}
	
	@Test
	public void deleteAll() {
		buildAndDelete(4,3,9,5,6,8,7,2,13,17);
	}
	
	@Test
	public void deleteWithRippleBalance() {
		buildAndDelete(3,4,7,13,6);
	}
	
	/**
	 * Perform a delete which does not result in any merges.
	 */
	@Test
	public void deleteInLeafNoBalance() {
		buildAndDelete(4);
	}
	
	@Test
	public void deleteInLeafRightEdge() {
		buildAndDelete(6);
		BTree.Reference ref = btree.openReadOnly().search(createKey(7));
		assertNotNull(ref);
	}
	
	@Test
	public void deleteInLeafLeftEdge() {
		buildAndDelete(5);
	}
	
	@Test
	public void deleteInLeafWithDrainFromLeft() {
		buildAndDelete(6,7);
	}
	
	@Test
	public void deleteInLeafWithDrainFromRight() {
		buildAndDelete(3,4,5);
	}
	
	@Test
	public void deleteInLeafWithDrainFromRightChild() {
		buildAndDelete(3,4,7,8);
	}
	
	@Test
	public void deleteInRightChildLeafWithDrainFromLeft() {
		buildAndDelete(9,13);
	}
	
	private void buildAndDelete(long... toDelete) {
		// first create a small tree
		long[] ids = new long[] {4,3,9,5,6,8,7,2,13,17};
//		((ReadOnlyTransactionBTree)btree.openReadOnly()).print();
//		System.out.println("----------------------------");
		for (long id : ids) {
			storeFetchOne(btree, id);
//			((ReadOnlyTransactionBTree)btree.openReadOnly()).print();
//			System.out.println("----------------------------");
		}
		BTreeBacking backing = ((BackedBTree)btree).backing();
		
		
		//                    (5,8)
		//          (2,3,4,5) (6,7,8) (9,13,17)
		
		//                    (4,6)
		//          (2,3,4) (5,6) (7,8,9,13,17)
		
		for (long d : toDelete)	{
			if (backing instanceof HeapBufferBacking) {
				System.out.printf("--- about to delete %d%n",d);
				((HeapBufferBacking) backing).dump(System.out);
			}
			deleteOne(d);
		}
	}
	
	private void deleteOne(long id) {
		// now delete from a node that won't become too small
		BTree.Key k = createKey(id);
		BTreeTransaction bt = btree.open();
		BTree.Reference refSearch = bt.search(k);
		assertNotNull(refSearch);
		BTree.Reference refDelete = bt.delete(k);
		assertNotNull(refDelete);
		assertEquals(refSearch, refDelete);
		bt.commit();
		bt.close();
		
		ReadOnlyBTreeTransaction btr = btree.openReadOnly();
		// search again and check that the item is no longer in the tree
		BTree.Reference refSearchAgain = btr.search(k);
		assertNull("id="+id+" should no longer exist",refSearchAgain);
		btr.close();
	}
	
	@Test
	public void splitLeaf() {
		// this depends on the capacity being 5
		long[] ids = new long[] {4,3,9,5,6,8,7,2,13,17};
		for (long id : ids) {
			
			// dump backing
			BTreeBacking backing = ((BackedBTree)btree).backing();
			if (backing instanceof HeapBufferBacking) {
				((HeapBufferBacking) backing).dump(System.out);
			}
			
			System.out.printf("Storing id: %d%n", id);
			storeFetchOne(btree, id);
		}
	}
	
	@Test
	public void splitRoot() {
		// 6*5 < 35 < 10*5 (i.e. only 2 inner nodes below root required)
		storeWithSplit(btree, 35);
		
		((ReadOnlyTransactionBTree)btree.openReadOnly()).print();
	}
	
	@Test
	public void splitInner() {
		// 10*5 < 70 < 15*5 (i.e. 3 inner nodes below root now required)
		storeWithSplit(btree, 70);
	}
	
	@Test
	public void storeFetch() {
		storeFetchOne(btree, 1);
	}
	
	@Test
	public void search() {
		byte[] value1 = random(100);
		byte[] value2 = random(100);
		
		BTree.Key key1 = new BTree.Key();
		key1.idx = 1;
		key1.idHigh = 1;
		
		BTree.Key key2 = new BTree.Key();
		key2.idx = 1;
		key2.idHigh = 2;
		
		BTreeTransaction bt = btree.open();
		BTree.Reference refStore1 = bt.store(key1, 0, ByteBuffer.wrap(value1));
		assertNotNull(refStore1);
		bt.commit();
		bt.close();
		
		bt = btree.open();
		BTree.Reference refStore2 = bt.store(key2, 0, ByteBuffer.wrap(value2));
		assertNotNull(refStore2);
		bt.commit();
		bt.close();
		
		assertFalse(refStore1.equals(refStore2));
		
		ReadOnlyBTreeTransaction btr = btree.openReadOnly();
		BTree.Reference refSearch = btr.search(key1);
		assertNotNull(refSearch);
		assertTrue(refSearch.size > 0);
		
		assertEquals(refStore1, refSearch);
		byte[] buf = new byte[(int)refSearch.size];
		
		int l = btr.fetch(refSearch,0,ByteBuffer.wrap(buf));
		assertTrue(l > 0);
		assertTrue(Arrays.equals(value1, buf));
		btr.close();
	}
	
	@Test
	public void keyCompare() {
		BTree.Key keyA = new BTree.Key();
		BTree.Key keyB = new BTree.Key();
		BTree.Key keyC = new BTree.Key();
		BTree.Key keyD = new BTree.Key();
		
		keyA.idx = 1;
		keyA.idHigh = 1;
		
		keyB.idx = 1;
		keyB.idHigh = 1;
		
		keyC.idx = 2;
		keyC.idHigh = 1;
		keyC.type = 1;
		
		keyD.idx = 2;
		keyD.idHigh = 1;
		keyD.type = 4;
		
		assertNotSame(keyA,keyB);
		assertEquals(keyA,keyB);
		assertEquals(0,keyA.compareTo(keyB));
		
		assertTrue(keyB.compareTo(keyC) < 0);
		assertTrue(keyC.compareTo(keyD) < 0);
	}

}
