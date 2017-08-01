/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.BTreeTestUtils;
import net.gethos.cohesion.storage.backing.BTreeBackingTransaction;
import net.gethos.cohesion.storage.backing.BTreeNode;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class HeapBufferBackingTest {
	
	private BTreeBackingTransaction transaction;
	private HeapBufferBacking backing;
	
	@Before
	public void setUp () {
		backing = new HeapBufferBacking(4096);
		
		transaction = backing.open();
	}
	
	@After
	public void tearDown() {
		if (transaction != null) {
			transaction.commit();
			transaction.close();
		}
	}
	
	@Test
	public void allocFree() {
		long iOffset,lOffset;
		BTreeNode n;
		
		
		// allocate and retrieve an index node
		iOffset = transaction.alloc(false);
		assertTrue(iOffset >= 0);
		n = transaction.retrieve(iOffset);
		assertNotNull(n);
		assertTrue(n instanceof BufferIndexNode);
		
		// allocate and retrieve a leaf node
		lOffset = transaction.alloc(true);
		assertTrue(lOffset >= 0);
		n = transaction.retrieve(lOffset);
		assertNotNull(n);
		assertTrue(n instanceof BufferLeafNode);
		
		// check that things differ
		assertFalse(iOffset == lOffset);
		
		// free the nodes
		long l;
		l = transaction.free(iOffset);
		assertTrue(l >= 0);
		l = transaction.free(lOffset);
		assertTrue(l >= 0);
	}
	
	@Test
	public void bufferType() {
		assertEquals(BufferNode.NodeType.UNKNOWN, BufferNode.NodeType.valueOf((byte)0));
		assertEquals(BufferNode.NodeType.UNKNOWN, BufferNode.NodeType.valueOf((byte)4));
		assertEquals(BufferNode.NodeType.INDEX, BufferNode.NodeType.valueOf((byte)1));
		assertEquals(BufferNode.NodeType.LEAF, BufferNode.NodeType.valueOf((byte)2));
		assertEquals(BufferNode.NodeType.SUPER, BufferNode.NodeType.valueOf((byte)3));
	}

	@Test
	public void scaling() {
		
		int testScaling = 100+BufferLeafNode.ITEM_ENTRY_SIZE;
		
		// scaling level
		assertEquals(742,BufferScaling.round(5, testScaling));
		assertEquals(232,BufferScaling.round(5, 40));
		
		assertEquals(4096,BufferScaling.round(20, testScaling));
		assertEquals(16384,BufferScaling.round(100, testScaling));
		assertEquals(28672,BufferScaling.round(200, testScaling));
		
		// edge
		assertEquals(1024,BufferScaling.round(1024, testScaling));
		
		// raw level
		assertEquals(4096,BufferScaling.round(2000, testScaling));
		assertEquals(4096,BufferScaling.round(4096, testScaling));
		assertEquals(8192,BufferScaling.round(4097, testScaling));
		assertEquals(8192,BufferScaling.round(6000, testScaling));
		assertEquals(8192,BufferScaling.round(8192, testScaling));
	}
	
	/**
	 * A quick test to double check when the 'for' loop increment
	 * is executed relative to the body of the loop, so that when
	 * we set the iteration variable to -1 the body will actual
	 * see the next value as 0.
	 */
	@Test
	public void loopWrap() {
		StringBuilder s = new StringBuilder();
		int x = 0;
		int wrap = 0;
		for (
				x = 0;
				x < 10;
				x++
			) {
			if (x > 5) {
				x = -1;
				wrap++;
				if (wrap >= 2)
					break;
				else
					continue;
			}
			s.append(Integer.toString(x));
		}
		assertEquals(2,wrap);
		assertEquals("012345012345",s.toString());
	}
	
	@Test
	public void balanceLeafPull() {
		long[] leftIds = new long[] {6,8,10};
		long[] rightIds = new long[] {12,18};
		
		balanceLeafPull(null, 0, leftIds, rightIds);
	}
	
	@Test
	public void balanceLeafPullRequired() {
		// test that pull works when needing to leave required space
		balanceLeafPull(BTreeTestUtils.createKey(7)/*idx 1*/, 300, new long[] {6,7,8}, new long[] {10,12,18});
		balanceLeafPull(BTreeTestUtils.createKey(13)/*idx 3*/, 200, new long[] {6,8,10,12}, new long[] {13,18});
	}
	
	private void balanceLeafPull(BTree.Key requiredKey, int requiredLength, long[] expectLeft, long[] expectRight) {
		// create a leaf and fill with data
		// create an sibling
		// balance
		int capacity = BufferScaling.round(5, 100+BufferLeafNode.ITEM_ENTRY_SIZE);
		BufferLeafNode s = BufferNode.allocateLeaf(capacity);
		BufferLeafNode n = BufferNode.allocateLeaf(capacity);
		
		long[] ids = new long[] {8,6,18,10,12};
		
		for (long i : ids) {
			int ni = n.realloc(BTreeTestUtils.createKey(i),100);
//			System.out.printf("after alloc of id=%d, ni=%d, n=%s%n",i,ni,n.dump());
			assertTrue(ni >= 0);
			int l = n.write(ni, 0, ByteBuffer.wrap(BTreeTestUtils.random(100)));
			assertEquals(100,l);
		}
		
		boolean ret = s.balance(n, requiredKey, requiredLength, false);
		assertTrue("pull failed :(", ret);
		
		long[] sids = new long[s.items()];
		for (int i = 0; i < s.items(); i++) sids[i] = s.key(i).idHigh;
		
		long[] nids = new long[n.items()];
		for (int i = 0; i < n.items(); i++) nids[i] = n.key(i).idHigh;
		
		String msgLeft = String.format("Expected left: %s but was %s",Arrays.toString(expectLeft), Arrays.toString(sids));
		String msgRight = String.format("Expected right: %s but was %s",Arrays.toString(expectRight), Arrays.toString(nids));
		
		assertEquals("Sizes differ - " + msgLeft, expectLeft.length, s.items());
		assertEquals("Sizes differ - " + msgRight, expectRight.length, n.items());
		
		assertTrue("Elements differ - " + msgLeft, Arrays.equals(expectLeft, sids));
		assertTrue("Elements differ - " + msgRight, Arrays.equals(expectRight, nids));
	}
	
	@Test
	public void balanceLeafPush() {
		// create a leaf and fill with data
		// create an sibling
		// balance
		int capacity = BufferScaling.round(5, 100+BufferLeafNode.ITEM_ENTRY_SIZE);
		BufferLeafNode n = BufferNode.allocateLeaf(capacity);
		BufferLeafNode s = BufferNode.allocateLeaf(capacity);
		
		long[] ids = new long[] {3,6,9};
		long[] empty = new long[] {};
		long[] leftIds = new long[] {3,6};
		long[] rightIds = new long[] {9};
		
		for (long i : leftIds) {
			int ni = n.realloc(BTreeTestUtils.createKey(i),100);
//			System.out.printf("after alloc of id=%d, ni=%d, n=%s%n",i,ni,n.dump());
			assertTrue(ni >= 0);
			int l = n.write(ni, 0, ByteBuffer.wrap(BTreeTestUtils.random(100)));
			assertEquals(100,l);
		}
		
		for (long i : rightIds) {
			int si = s.realloc(BTreeTestUtils.createKey(i),100);
//			System.out.printf("after alloc of id=%d, ni=%d, n=%s%n",i,ni,n.dump());
			assertTrue(si >= 0);
			int l = s.write(si, 0, ByteBuffer.wrap(BTreeTestUtils.random(100)));
			assertEquals(100,l);
		}
		
		boolean ret = s.balance(n,true);
		assertTrue(ret);

		assertEquals(3, n.items());
		assertEquals(0, s.items());
		
		long[] nids = new long[3];
		for (int i = 0; i < n.items(); i++) nids[i] = n.key(i).idHigh;
		
		long[] sids = new long[0];
		
		assertTrue(String.format("Expected node: %s but was %s",Arrays.toString(ids), Arrays.toString(nids)), Arrays.equals(ids, nids));
		assertTrue(String.format("Expected sibling: %s but was %s",Arrays.toString(empty), Arrays.toString(empty)), Arrays.equals(empty, sids));
	}
}
