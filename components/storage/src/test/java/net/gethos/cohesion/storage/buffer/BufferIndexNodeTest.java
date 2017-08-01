/**
 * Cohesion Framework - Storage Library
 * Copyright (c) 2017 - Stewart Gebbie, Gethos. Licensed under the MIT licence.
 * vim: set ts=4 sw=0:
 */
package net.gethos.cohesion.storage.buffer;

import net.gethos.cohesion.storage.BTree;
import net.gethos.cohesion.storage.StorageConstants;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author {@literal Stewart Gebbie <sgebbie@gethos.net>}
 *
 */
public class BufferIndexNodeTest {
	
//	private BTree.Key nKey;
	
	private BufferIndexNode corpusCopy;
	
	@Before
	public void setUp() {
//		nKey = BTree.Key.parseKey("0001:ecc7ef42e5ce0378d44e25b0ee8dc4bf90bf8668:0000:0000000000000000");

		corpusCopy = BufferNode.allocateIndex(StorageConstants.DEFAULT_NODE_CAPACITY);
		addItem(corpusCopy,"0001:e9c5e5c772394522aff438bbee3a94284b84f69f:0000:0000000000000000");
		addItem(corpusCopy,"0001:e9f4ab521041fed11f91bd2bac938b5fe8d82ca7:0000:0000000000000000");
		addItem(corpusCopy,"0001:ea09f7e711962b63b6b1e10a4c799a4e8a6f50e1:0000:0000000000000000");
		addItem(corpusCopy,"0001:ea4f941b31f1be7473d82be8087321f06b0756c2:0000:0000000000000000");
		addItem(corpusCopy,"0001:eaa9ea623fe81a1085abc6cafe23d2e51feecc1b:0000:0000000000000000");
		addItem(corpusCopy,"0001:ecb1ef6c2fb6a44b15ef083ba89cf38f7ec6bba4:0000:0000000000000000");

		addItem(corpusCopy,"0001:ecec1690ad594f685e1707138c4187f079ec941e:0000:0000000000000000");
		addItem(corpusCopy,"0001:edd3dcb1755ac9fb4b6705e675b9335a25cce394:0000:0000000000000000");
		addItem(corpusCopy,"0001:edf95ec8821972a3fe6a10995cbc50215ce02c1a:0000:0000000000000000");
		addItem(corpusCopy,"0001:edfd6ae69df6ad701a2e6ebe800da2d714ffec91:0000:0000000000000000");
		addItem(corpusCopy,"0001:eeb6cdc1646d1775ac1f4506c87140ae6c0baf15:0000:0000000000000000");
		addItem(corpusCopy,"0001:eebfbb871251d978a96be372706906b85ab0cd43:0000:0000000000000000");
		addItem(corpusCopy,"0001:eedc08bd0490b2d1d51a7c707f86830a5b4d448c:0000:0000000000000000");
		addItem(corpusCopy,"0001:efdff4c45730e94d50ed34c0ff075b9b5465ae51:0000:0000000000000000");
		addItem(corpusCopy,"0001:f02d254efa46fe606e277e65c1144564fb56f4bd:0000:0000000000000000");
		addItem(corpusCopy,"0001:f038e6910823f8c43d70ffb67dc9e8caada6eba0:0000:0000000000000000");
		addItem(corpusCopy,"0001:f0b9734a3ca9cb6a9f88e55cf8cb4ccf1ba8925a:0000:0000000000000000");
		addItem(corpusCopy,"0001:f0cb483db0b5eab8a2fe180aa4a1e5fdff2025e9:0000:0000000000000000");
		addItem(corpusCopy,"0001:f188bf26160506b42a3e1f633c14fc66a8221aaf:0000:0000000000000000");
		addItem(corpusCopy,"0001:f1f2a323db9f4e38193f2f1e94601dfed6a3d19f:0000:0000000000000000");
		addItem(corpusCopy,"0001:f281b791b14f901ee31f818cb1ade76937a99db9:0000:0000000000000000");
		addItem(corpusCopy,"0001:f281ceb0c92d9c51aff027177366e63f9c95cf4d:0000:0000000000000000");
		addItem(corpusCopy,"0001:f2a8b917cf143f1ab0ed02a5fad83c6c9d953c12:0000:0000000000000000");
	}
	
	private BufferIndexNode emptyNode() {
		BufferIndexNode emptyNode = BufferNode.allocateIndex(StorageConstants.DEFAULT_NODE_CAPACITY);
		return emptyNode;
	}
	
//	private BufferIndexNode populatedNode() {
//		BufferIndexNode populatedNode = BufferNode.allocateIndex(corpusCopy.capacity());
//		
//		for (int i = 0; i < corpusCopy.items(); i++) {
//			int ni = populatedNode.alloc(corpusCopy.key(i));
//			populatedNode.write(ni, corpusCopy.offset(i));
//		}
//		
//		return populatedNode;
//	}
	
	
	private void addItem(BufferIndexNode node, String ktext) {
		BTree.Key k = BTree.Key.parseKey(ktext);
		int r = node.alloc(k);
		assertTrue(r >= 0);
	}
	
	
	@Test
	public void compressRightChild() {

		BufferIndexNode s = emptyNode();
		addItem(s,"0001:0000000000000002000000000000000000000000:0000:0000000000000000");
		addItem(s,"0001:0000000000000005000000000000000000000000:0000:0000000000000000");
		addItem(s,"0001:0000000000000006000000000000000000000000:0000:0000000000000000");
		addItem(s,"0001:0000000000000007000000000000000000000000:0000:0000000000000000");
		addItem(s,"0001:0000000000000008000000000000000000000000:0000:0000000000000000");

		BufferIndexNode n = emptyNode();
		addItem(n,"0001:000000000000000d000000000000000000000000:0000:0000000000000000");
		addItem(n,"0001:0000000000000011000000000000000000000000:0000:0000000000000000");
		n.writeRight(123L);
		
//		System.out.printf("before s = %s%n", s);
//		System.out.printf("before n = %s%n", n);
		
		assertTrue(n.isCompressibleWith(s));
		boolean ok = n.balance(s, true);
		assertTrue(ok);
		
//		System.out.printf("after s = %s%n", s);
//		System.out.printf("after n = %s%n", n);
		

		assertTrue(n.items() == 0);
		assertTrue(n.children() == 0);
		assertEquals(BufferIndexNode.NO_RIGHT_HAND_CHILD,n.rightChild());
		
		assertTrue(s.items() == 7);
		assertTrue(s.children() == 8);
		assertEquals(123L,s.rightChild());
	}
	
}
